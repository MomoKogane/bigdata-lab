#!/bin/bash

# ===== ���ò��� =====
# ����·��
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"
HBASE_HOME="/usr/local/hbase"
MYSQL_PWD="hive"

# ���Ŀ¼��ʹ�þ���·����
OUTPUT_DIR="/usr/local/output/bigdata-user-table"

# HDFSĿ¼ - ʹ��ʵ�����������"hdfs"
HDFS_HOST=$(hdfs getconf -confKey fs.defaultFS | sed 's|^hdfs://||; s|:.*||')
HBASE_TMP_DIR="/user/hbase/bigdata_tmp"
HDFS_INPUT_DIR="/user/hbase/bigdata_input"
HDFS_OUTPUT_DIR="/user/hbase/bigdata_output_hfile"

# �����ݼ��ֿ����
BLOCK_SIZE=500000  # MySQLÿ��50����
HIVE_REDUCERS=20   # ���ݼ�Ⱥ��ģ����

# ��־�ļ�
LOG_FILE="${OUTPUT_DIR}/migration_$(date +%Y%m%d_%H%M%S).log"

# ===== ��־���� =====
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# ===== ��ʼ�� =====
log "===== ��ʼ������ ====="

# �������Ŀ¼
sudo mkdir -p ${OUTPUT_DIR} 2>/dev/null
sudo chown -R $USER:$USER ${OUTPUT_DIR} 2>/dev/null
mkdir -p ${OUTPUT_DIR} 2>/dev/null

# ���������
log "--- �������� ---"
rm -f ${OUTPUT_DIR}/* 2>/dev/null
hdfs dfs -rm -r ${HDFS_INPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -rm -r ${HBASE_TMP_DIR} >> "$LOG_FILE" 2>&1

# ȷ��HDFSĿ¼����
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -mkdir -p ${HDFS_OUTPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -mkdir -p ${HBASE_TMP_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -chown hadoop:hadoop ${HBASE_TMP_DIR} >> "$LOG_FILE" 2>&1

# ===== 1. �������� =====
log "===== �������� ====="

# ��鲢����MySQL
log "--- ��鲢����MySQL���� ---"
if ! mysqladmin ping -u hive -p${MYSQL_PWD} >/dev/null 2>&1; then
    if command -v systemctl &>/dev/null; then
        sudo systemctl start mysql >> "$LOG_FILE" 2>&1
    else
        sudo service mysql start >> "$LOG_FILE" 2>&1
    fi
    sleep 5  # �ȴ���������
    
    # ��֤����
    if ! mysqladmin ping -u hive -p${MYSQL_PWD} >/dev/null 2>&1; then
        log "!!! MySQL��������ʧ�ܣ��˳��ű� !!!"
        exit 1
    else
        log "MySQL���������ɹ�"
    fi
else
    log "MySQL����������"
fi

# ��鲢����Hadoop
log "--- ��鲢����Hadoop���� ---"
if ! jps | grep -q NameNode || ! jps | grep -q ResourceManager; then
    cd ${HADOOP_DIR}
    ./sbin/start-dfs.sh >> "$LOG_FILE" 2>&1
    ./sbin/start-yarn.sh >> "$LOG_FILE" 2>&1
    
    # �ȴ������ȶ�
    sleep 10
    
    # ��֤����
    if ! jps | grep -q NameNode || ! jps | grep -q ResourceManager; then
        log "!!! Hadoop��������ʧ�ܣ��˳��ű� !!!"
        jps >> "$LOG_FILE" 2>&1
        exit 1
    else
        log "Hadoop���������ɹ�"
    fi
else
    log "Hadoop����������"
fi

# ��鲢����HBase
log "--- ��鲢����HBase���� ---"
if ! jps | grep -q HMaster; then
    ${HBASE_HOME}/bin/start-hbase.sh >> "$LOG_FILE" 2>&1
    sleep 5
    
    # ��֤����
    if ! jps | grep -q HMaster; then
        log "!!! HBase��������ʧ�ܣ��˳��ű� !!!"
        jps >> "$LOG_FILE" 2>&1
        exit 1
    else
        log "HBase���������ɹ�"
    fi
else
    log "HBase����������"
fi

# ===== 2. Hive���ݵ��� =====
log "===== Hive���ݵ��� ====="

# ������ʱ���������ݣ������ݼ��Ż���
log "--- ��Hive�д�����ʱ���������� ---"
${HIVE_DIR}/bin/hive -e "
-- �����Ż�����
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.optimize.sort.dynamic.partition=true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

USE dblab;

-- ɾ������ʱ��������ڣ�
DROP TABLE IF EXISTS raw_user_action;

-- ������ʱ��ORC��ʽ�������ܣ�
CREATE TABLE raw_user_action (
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
" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "!!! Hive���ݵ���ʧ�� !!!"
    exit 1
fi

# ��֤�������
log "--- �鿴�����ļ����� ---"
ls ${OUTPUT_DIR} | wc -l | tee -a "$LOG_FILE"
log "--- �鿴��������ǰ5�� ---"
find ${OUTPUT_DIR} -type f -exec head -n 5 {} \; | tee -a "$LOG_FILE"

# ===== 3. �������ݵ�MySQL =====
log "===== ���ݵ���MySQL ====="

# ��¼MySQL��������������ԭʼ�ṹһ�£�
log "--- ����/�ؽ�MySQL��ṹ ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;

-- ɾ���ɱ�������ڣ�
DROP TABLE IF EXISTS raw_user_action;

-- ��������ԭʼ�ṹ��ȫһ�£�
CREATE TABLE raw_user_action (
    id VARCHAR(50),
    uid VARCHAR(50),
    item_id VARCHAR(50),
    behavior_type VARCHAR(50),
    item_category VARCHAR(50),
    visit_date DATE,
    province VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ���ü����ٵ���
SET unique_checks=0;
SET foreign_key_checks=0;
" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "!!! MySQL����ʧ�� !!!"
    exit 1
fi

# �ֿ鵼�����ݣ������ݼ��Ż���
log "--- �ֿ鵼������ ---"
files=(${OUTPUT_DIR}/00*)
file_count=${#files[@]}
counter=0

for file in "${files[@]}"; do
    counter=$((counter+1))
    log "�����ļ��� [$counter/$file_count]: $(basename $file)"
    
    # ����ǰ�������ڸ�ʽ
    awk -F',' 'BEGIN {OFS=","} {
        # �������ڸ�ʽ��ȷ����YYYY-MM-DD
        split($6, date_parts, "-");
        if (length(date_parts[1]) == 4) {
            $6 = sprintf("%s-%02d-%02d", date_parts[1], date_parts[2], date_parts[3]);
        }
        print $0
    }' "$file" > "${file}_processed"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    LOAD DATA LOCAL INFILE '${file}_processed'
    INTO TABLE raw_user_action
    CHARACTER SET utf8
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    (id, uid, item_id, behavior_type, item_category, visit_date, province);
    " >> "$LOG_FILE" 2>&1
    
    if [ $? -ne 0 ]; then
        log "!!! �ļ�����ʧ��: $(basename $file) !!!"
    else
        rm "${file}_processed"
    fi
    
    # ÿ����5���ļ���ʾ����
    if [ $((counter % 5)) -eq 0 ]; then
        mysql -u hive --password=${MYSQL_PWD} -e "USE dblab; SELECT COUNT(*) AS total_rows FROM raw_user_action;" | tee -a "$LOG_FILE"
    fi
done

# �������ü��
log "--- ���������Լ�� ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SET unique_checks=1;
SET foreign_key_checks=1;
" >> "$LOG_FILE" 2>&1

# ������֤
log "--- MySQL��ͳ�� ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SELECT COUNT(*) AS total_rows FROM raw_user_action;
SELECT MIN(visit_date) AS min_date, MAX(visit_date) AS max_date FROM raw_user_action;
" | tee -a "$LOG_FILE"

# ===== 4. �������ݵ�HBase =====
log "===== ���ݵ���HBase ====="

# ��MySQL�������ݵ����أ��ֿ鴦��
log "--- ��MySQL�ֿ鵼������ ---"
total_rows=$(mysql -u hive --password=${MYSQL_PWD} -sN -e "USE dblab; SELECT COUNT(*) FROM raw_user_action;" 2>>"$LOG_FILE")
blocks=$(( (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE ))
log "������: $total_rows, �ֿ���: $blocks"

for ((i=0; i<blocks; i++)); do
    offset=$((i * BLOCK_SIZE))
    log "�������� [$((i+1))/$blocks]: �� $offset - $((offset + BLOCK_SIZE))"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    SELECT * FROM raw_user_action 
    LIMIT ${BLOCK_SIZE} OFFSET ${offset}
    " > ${OUTPUT_DIR}/raw_user_action_${i}.tsv 2>>"$LOG_FILE"
    
    # ����ļ��Ƿ�Ϊ��
    if [ ! -s "${OUTPUT_DIR}/raw_user_action_${i}.tsv" ]; then
        log "!!! �����ļ�Ϊ��: raw_user_action_${i}.tsv !!!"
    fi
done

# �ϴ���HDFS
log "--- �ϴ����ݵ�HDFS ---"
hdfs dfs -put ${OUTPUT_DIR}/raw_user_action_*.tsv ${HDFS_INPUT_DIR}/ >> "$LOG_FILE" 2>&1

# �޸�HBase�����
log "--- ��HBase�д���/�ؽ��� ---"
${HBASE_HOME}/bin/hbase shell <<EOF 2>>"$LOG_FILE"
# �����Ƿ���ڣ������������ò�ɾ��
if exists 'raw_user_action'
    disable 'raw_user_action'
    drop 'raw_user_action'
end
# �����±�
create 'raw_user_action', 
  {NAME => 'f1', VERSIONS => 5}, 
  {SPLITS => ['1000000', '3000000', '5000000', '7000000', '9000000']}
EOF

if [ $? -ne 0 ]; then
    log "!!! HBase����ʧ�� !!!"
    exit 1
fi

# ��������HFile - ʹ��ʵ��������
log "--- ��������HFile ---"
# ȷ��HFile���Ŀ¼�����ڣ�ImportTsvҪ��Ŀ¼�����ڣ�
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR}/* >> "$LOG_FILE" 2>&1

for file in $(hdfs dfs -ls ${HDFS_INPUT_DIR} | grep tsv | awk '{print $NF}'); do
    log "�����ļ�: $file"
    # Ϊÿ���ļ�����������HFile���Ŀ¼
    hfile_output="${HDFS_OUTPUT_DIR}/$(basename ${file} .tsv)_hfiles"
    # ɾ�����ܴ��ڵľ�Ŀ¼
    hdfs dfs -rm -r ${hfile_output} >> "$LOG_FILE" 2>&1

    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
      -Dimporttsv.rowkey.position=0 \
      -Dimporttsv.separator=9 \
      -Dimporttsv.columns="HBASE_ROW_KEY,f1:uid,f1:item_id,f1:behavior_type,f1:item_category,f1:visit_date,f1:province" \
      -Dimporttsv.bulk.output="${hfile_output}" \
      raw_user_action \
      ${file} >> "$LOG_FILE" 2>&1

    if [ $? -ne 0 ]; then
        log "!!! ImportTsvʧ��: $(basename $file) !!!"
        # ��¼ʧ�ܣ����������������ļ�
    else
        log "����HFile�ɹ�: ${hfile_output}"
    fi
done

# ��������HFile
log "--- ��������HFile��HBase ---"
for hfile_dir in $(hdfs dfs -ls ${HDFS_OUTPUT_DIR} | grep hfiles | awk '{print $NF}'); do
    log "����HFile: $hfile_dir"
    
    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
      ${hfile_dir} \
      raw_user_action >> "$LOG_FILE" 2>&1
    
    if [ $? -ne 0 ]; then
        log "!!! LoadIncrementalHFilesʧ��: $(basename $hfile_dir) !!!"
    fi
done

# ������֤
log "--- HBase����֤ ---"
${HBASE_HOME}/bin/hbase shell <<EOF 2>>"$LOG_FILE"
count 'raw_user_action', INTERVAL => 1000000
exit
EOF

log "===== �����ݼ����ݻ������ ====="
log "Hive��: dblab.raw_user_action"
log "MySQL��: dblab.raw_user_action (�û�:hive, ���ݿ�:dblab)"
log "HBase��: raw_user_action"
log "���Ŀ¼: ${OUTPUT_DIR}"
log "������־: ${LOG_FILE}"