#!/bin/bash

# �������·����ʹ���û��ɶ�дĿ¼��
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"
MYSQL_PWD="hive"  # MySQL hive�û�����
OUTPUT_DIR="/home/hadoop/bigdata-output"  # ��Ϊ�û���Ŀ¼�µ�·��
HBASE_TMP_DIR="/user/hbase/bigdata_tmp"
HDFS_INPUT_DIR="/user/hbase/bigdata_input"
HDFS_OUTPUT_DIR="/user/hbase/bigdata_output_hfile"

# �������Ŀ¼���û���Ŀ¼�������ԱȨ�ޣ�
mkdir -p ${OUTPUT_DIR}

# 1. ��������
echo "===== �������� ====="

# ����MySQL���Զ��������룬��ȷ��mysql����֧��--password=ѡ�
echo "--- ����MySQL���� ---"
if command -v systemctl &>/dev/null; then
    systemctl start mysql
else
    service mysql start
fi

# ����Hadoop��hadoop�û���Ȩ�ޣ�
echo "--- ����Hadoop���� ---"
cd ${HADOOP_DIR}
./sbin/start-dfs.sh
./sbin/start-yarn.sh

# ����HBase
echo "--- ����HBase���� ---"
${HBASE_HOME}/bin/start-hbase.sh


# 2. Hive���ݵ���������
echo "===== Hive���ݵ��� ====="

# ����Hiveִ�в�������ȷʹ��hive�û����룩
echo "--- ��Hive�д�����ʱ���������� ---"
${HIVE_DIR}/bin/hive -e "
-- ʹ��dblab���ݿ�
USE dblab;

-- ���������ݼ���ʱ��
CREATE TABLE IF NOT EXISTS raw_user_action (
    id STRING,
    uid STRING,
    item_id STRING,
    behavior_type STRING,
    item_category STRING,
    visit_date DATE,
    province STRING
) COMMENT '�����ݼ���ʱ��'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- ��raw_user�������ݵ���ʱ��
INSERT OVERWRITE TABLE raw_user_action SELECT * FROM raw_user;

-- �������ݵ����أ��û���Ŀ¼·����
SET mapred.reduce.tasks=1;
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}/bigdata-user-table'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
SELECT * FROM dblab.raw_user_action;
"

# ��֤�������
echo "--- �鿴��������ǰ10�� ---"
head ${OUTPUT_DIR}/bigdata-user-table/000000_0


# 3. �������ݵ�MySQL����ȷ���������
echo "===== ���ݵ���MySQL ====="

# ��¼MySQL��������
echo "--- ����MySQL���������� ---"
mysql -u hive --password=${MYSQL_PWD} -e "
-- ʹ��dblab���ݿ�
USE dblab;

-- ���������ݼ���
CREATE TABLE IF NOT EXISTS raw_user_action (
    id VARCHAR(50),
    uid VARCHAR(50),
    item_id VARCHAR(50),
    behavior_type VARCHAR(50),
    item_category VARCHAR(50),
    visit_date DATE,
    province VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ��ձ�
TRUNCATE TABLE raw_user_action;

-- �������ݣ��û���Ŀ¼·����
LOAD DATA LOCAL INFILE '${OUTPUT_DIR}/bigdata-user-table/000000_0'
INTO TABLE raw_user_action
CHARACTER SET utf8
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@id, @uid, @item_id, @behavior_type, @item_category, @date_str, @province)
SET 
id = @id,
uid = @uid,
item_id = @item_id,
behavior_type = @behavior_type,
item_category = @item_category,
visit_date = STR_TO_DATE(@date_str, '%Y-%m-%d'),
province = @province;
"

# ��֤MySQL����
echo "--- �鿴MySQL��ǰ10����¼ ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SELECT * FROM raw_user_action LIMIT 10;
"


# 4. ��MySQL����������HBase
echo "===== ���ݵ���HBase ====="

# ��MySQL�������ݵ����أ��Զ��������룩
echo "--- ��MySQL�������� ---"
mysql -u hive --password=${MYSQL_PWD} -e "SELECT * FROM dblab.raw_user_action" > ${OUTPUT_DIR}/raw_user_action.tsv

# ɾ����ͷ
sed -i '1d' ${OUTPUT_DIR}/raw_user_action.tsv

# �ϴ���HDFS��hadoop�û���Ȩ�ޣ�
echo "--- �ϴ����ݵ�HDFS ---"
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR}
hdfs dfs -put ${OUTPUT_DIR}/raw_user_action.tsv ${HDFS_INPUT_DIR}

# ��HBase�д�����
echo "--- ��HBase�д����� ---"
${HBASE_HOME}/bin/hbase shell <<EOF
create 'raw_user_action', {NAME => 'f1', VERSIONS => 5}
exit
EOF

# ����HBase��ʱĿ¼��HDFS������hadoop�û�ִ�У�
echo "--- ����HBase��ʱĿ¼ ---"
hdfs dfs -mkdir -p ${HBASE_TMP_DIR}
hdfs dfs -chown hadoop:hadoop ${HBASE_TMP_DIR}

# ɾ���ɵ�HFile���
echo "--- ��������� ---"
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR} 2>/dev/null

# ʹ��ImportTsv����HFile
echo "--- ����HFile ---"
${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dhbase.fs.tmp.dir=file://${HBASE_TMP_DIR} \
  -Dhbase.rootdir=file://${HBASE_TMP_DIR} \
  -Dimporttsv.separator=9 \
  -Dimporttsv.columns="HBASE_ROW_KEY,f1:uid,f1:item_id,f1:behavior_type,f1:item_category,f1:visit_date,f1:province" \
  -Dimporttsv.bulk.output=hdfs://localhost:9000${HDFS_OUTPUT_DIR} \
  raw_user_action \
  hdfs://localhost:9000${HDFS_INPUT_DIR}/raw_user_action.tsv

# ��HFile���ص�HBase
echo "--- ����HFile��HBase ---"
${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
  hdfs://localhost:9000${HDFS_OUTPUT_DIR} \
  raw_user_action

# ��֤HBase����
echo "--- �鿴HBase��ǰ5����¼ ---"
${HBASE_HOME}/bin/hbase shell <<EOF
scan 'raw_user_action', {LIMIT => 5}
exit
EOF

echo "===== �����ݼ����ݻ������ ====="
echo "Hive��: dblab.raw_user_action"
echo "MySQL��: dblab.raw_user_action (�û�:hive, ���ݿ�:dblab)"
echo "HBase��: raw_user_action"