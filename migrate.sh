#!/bin/bash

# ===== ���ò��� =====
# ����·��
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"
HBASE_HOME="/usr/local/hbase"
MYSQL_PWD="hive"

# ���Ŀ¼��ʹ�þ���·����
OUTPUT_DIR="/usr/local/output/bigdata-user-table"

# HDFSĿ¼
HBASE_TMP_DIR="/user/hbase/bigdata_tmp"
HDFS_INPUT_DIR="/user/hbase/bigdata_input"
HDFS_OUTPUT_DIR="/user/hbase/bigdata_output_hfile"

# �����ݼ��ֿ����
BLOCK_SIZE=500000  # MySQLÿ��50����
HIVE_REDUCERS=20   # ���ݼ�Ⱥ��ģ����

# ===== ��ʼ�� =====
echo "===== ��ʼ������ ====="
# �������Ŀ¼����ҪsudoȨ�ޣ�
sudo mkdir -p ${OUTPUT_DIR}
sudo chown -R $USER:$USER ${OUTPUT_DIR}

# ���������
echo "--- �������� ---"
rm -f ${OUTPUT_DIR}/* 2>/dev/null
hdfs dfs -rm -r ${HDFS_INPUT_DIR} 2>/dev/null
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR} 2>/dev/null
hdfs dfs -rm -r ${HBASE_TMP_DIR} 2>/dev/null

# ===== 1. �������� =====
echo "===== �������� ====="

# ����MySQL
echo "--- ����MySQL���� ---"
if command -v systemctl &>/dev/null; then
    sudo systemctl start mysql
else
    sudo service mysql start
fi
sleep 5  # �ȴ���������

# ����Hadoop
echo "--- ����Hadoop���� ---"
cd ${HADOOP_DIR}
./sbin/start-dfs.sh
./sbin/start-yarn.sh
if [ $? -ne 0 ]; then
    echo "--- Hadoop��������ʧ�ܣ��˳��ű� ---"
    exit 1
fi
sleep 10  # �ȴ������ȶ�

# ����HBase
echo "--- ����HBase���� ---"
${HBASE_HOME}/bin/start-hbase.sh
if [ $? -ne 0 ]; then
    echo "--- HBase��������ʧ�ܣ��˳��ű� ---"
    exit 1
fi
sleep 5

# ===== 2. Hive���ݵ��� =====
echo "===== Hive���ݵ��� ====="

# ������ʱ���������ݣ������ݼ��Ż���
echo "--- ��Hive�д�����ʱ���������� ---"
${HIVE_DIR}/bin/hive -e "
-- �����Ż�����
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.optimize.sort.dynamic.partition=true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

USE dblab;

-- ������ʱ��ORC��ʽ�������ܣ�
CREATE TABLE IF NOT EXISTS raw_user_action (
    id STRING,
    uid STRING,
    item_id STRING,
    behavior_type STRING,
    item_category STRING,
    visit_date DATE,
    province STRING
) COMMENT '�����ݼ���ʱ��'
STORED AS ORC;

-- ��raw_user�������ݣ����д���
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
INSERT OVERWRITE TABLE raw_user_action SELECT * FROM raw_user;

-- �������ݵ����أ����ļ������
SET mapred.reduce.tasks=${HIVE_REDUCERS};
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
SELECT * FROM raw_user_action;
"

# ��֤�������
echo "--- �鿴�����ļ����� ---"
ls ${OUTPUT_DIR} | wc -l
echo "--- �鿴��������ǰ5�� ---"
find ${OUTPUT_DIR} -type f -print0 | xargs -0 head -n 5

# ===== 3. �������ݵ�MySQL =====
echo "===== ���ݵ���MySQL ====="

# ��¼MySQL��������������ԭʼ�ṹһ�£�
echo "--- ����MySQL��ṹ ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;

-- ��������ԭʼ�ṹ��ȫһ�£�
CREATE TABLE IF NOT EXISTS raw_user_action (
    id VARCHAR(50),
    uid VARCHAR(50),
    item_id VARCHAR(50),
    behavior_type VARCHAR(50),
    item_category VARCHAR(50),
    visit_date DATE,  -- ȷ��DATE����
    province VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

TRUNCATE TABLE raw_user_action;

-- ���ü����ٵ���
SET unique_checks=0;
SET foreign_key_checks=0;
"

# �ֿ鵼�����ݣ������ݼ��Ż���
echo "--- �ֿ鵼������ ---"
file_count=$(ls ${OUTPUT_DIR}/00* | wc -l)
counter=0

for file in $(ls ${OUTPUT_DIR}/00*); do
    counter=$((counter+1))
    echo "�����ļ��� [$counter/$file_count]: $(basename $file)"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    LOAD DATA LOCAL INFILE '${file}'
    INTO TABLE raw_user_action
    CHARACTER SET utf8
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    (id, uid, item_id, behavior_type, item_category, @date_var, province)
    SET visit_date = STR_TO_DATE(@date_var, '%Y-%m-%d');  -- ȷ�����ڸ�ʽ��ȷ
    "
    
    # ÿ����5���ļ���ʾ����
    if [ $((counter % 5)) -eq 0 ]; then
        mysql -u hive --password=${MYSQL_PWD} -e "USE dblab; SELECT COUNT(*) AS total_rows FROM raw_user_action;"
    fi
done

# �������ü��
echo "--- ���������Լ�� ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SET unique_checks=1;
SET foreign_key_checks=1;
"

# ������֤
echo "--- MySQL��ͳ�� ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SELECT COUNT(*) AS total_rows FROM raw_user_action;
SELECT MIN(visit_date) AS min_date, MAX(visit_date) AS max_date FROM raw_user_action;
"

# ===== 4. �������ݵ�HBase =====
echo "===== ���ݵ���HBase ====="

# ��MySQL�������ݵ����أ��ֿ鴦��
echo "--- ��MySQL�ֿ鵼������ ---"
total_rows=$(mysql -u hive --password=${MYSQL_PWD} -sN -e "USE dblab; SELECT COUNT(*) FROM raw_user_action;")
blocks=$(( (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE ))

for ((i=0; i<blocks; i++)); do
    offset=$((i * BLOCK_SIZE))
    echo "�������� [$((i+1))/$blocks]: �� $offset - $((offset + BLOCK_SIZE))"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    SELECT * FROM raw_user_action 
    LIMIT ${BLOCK_SIZE} OFFSET ${offset}
    " > ${OUTPUT_DIR}/raw_user_action_${i}.tsv
done

# �ϴ���HDFS
echo "--- �ϴ����ݵ�HDFS ---"
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR}
hdfs dfs -put ${OUTPUT_DIR}/raw_user_action_*.tsv ${HDFS_INPUT_DIR}/

# ��HBase�д�������Ԥ������
echo "--- ��HBase�д����� ---"
${HBASE_HOME}/bin/hbase shell <<EOF
disable 'raw_user_action'
drop 'raw_user_action'
create 'raw_user_action', 
  {NAME => 'f1', VERSIONS => 5}, 
  {SPLITS => ['1000000', '3000000', '5000000', '7000000', '9000000']}  # Ԥ����
EOF

# ����HBase��ʱĿ¼
echo "--- ����HBase��ʱĿ¼ ---"
hdfs dfs -mkdir -p ${HBASE_TMP_DIR}
hdfs dfs -chown hadoop:hadoop ${HBASE_TMP_DIR}

# ��������HFile
echo "--- ��������HFile ---"
for file in $(hdfs dfs -ls ${HDFS_INPUT_DIR} | grep tsv | awk '{print $NF}'); do
    echo "�����ļ�: $file"
    
    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
      -Dimporttsv.rowkey.position=0 \
      -Dimporttsv.separator=9 \
      -Dimporttsv.columns="HBASE_ROW_KEY,f1:uid,f1:item_id,f1:behavior_type,f1:item_category,f1:visit_date,f1:province" \
      -Dimporttsv.bulk.output=hdfs://$(hdfs getconf -confKey fs.defaultFS)${HDFS_OUTPUT_DIR}/$(basename ${file})_hfiles \
      raw_user_action \
      ${file}
done

# ��������HFile
echo "--- ��������HFile��HBase ---"
for hfile_dir in $(hdfs dfs -ls ${HDFS_OUTPUT_DIR} | grep hfiles | awk '{print $NF}'); do
    echo "����HFile: $hfile_dir"
    
    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
      ${hfile_dir} \
      raw_user_action
done

# ������֤
echo "--- HBase����֤ ---"
${HBASE_HOME}/bin/hbase shell <<EOF
count 'raw_user_action', INTERVAL => 1000000
exit
EOF

echo "===== �����ݼ����ݻ������ ====="
echo "Hive��: dblab.raw_user_action"
echo "MySQL��: dblab.raw_user_action (�û�:hive, ���ݿ�:dblab)"
echo "HBase��: raw_user_action"
echo "���Ŀ¼: ${OUTPUT_DIR}"