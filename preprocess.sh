#!/bin/bash

# �������·��
DATASET_DIR="/usr/local/bigdatacase/dataset"
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"

# 1. ����pre_deal.sh����ԭʼ����
echo "===== ��ʼ����Ԥ���� ====="
cd ${DATASET_DIR}
echo "--- ִ��pre_deal.sh����raw_user.csv ---"
bash ./pre_deal.sh raw_user.csv raw_user_table.txt

# ��֤Ԥ���������鿴ǰ10�У�
echo "--- Ԥ���������Ԥ�� ---"
head -10 raw_user_table.txt


# 2. �ϴ���HDFS����֤
echo "===== ��ʼHDFS���� ====="
cd ${HADOOP_DIR}

# ����Hadoop
echo "--- ��鲢����Hadoop ---"
jps | grep -q NameNode || ./sbin/start-all.sh

# �ϴ��ļ���HDFS
echo "--- �ϴ�raw_user_table.txt��HDFS ---"
./bin/hdfs dfs -put ${DATASET_DIR}/raw_user_table.txt /bigdatacase/dataset

# �鿴ǰ15��
echo "--- HDFS�ļ�����Ԥ�� ---"
./bin/hdfs dfs -cat /bigdatacase/dataset/raw_user_table.txt | head -15


# 3. ��Hive�д����ⲿ����֤
echo "===== ��ʼHive���� ====="

# ����MySQL
echo "--- ��鲢����MySQL ---"
service mysql status || service mysql start

# ����Hiveִ�н���Ͳ�ѯ
cd ${HIVE_DIR}
echo "--- ��Hive�д����ⲿ����֤���� ---"
./bin/hive -e "
USE dblab;

-- �����ⲿ��
CREATE EXTERNAL TABLE IF NOT EXISTS raw_user (
    id INT,
    uid STRING,
    item_id STRING,
    behavior_type INT,
    item_category STRING,
    visit_date DATE,
    province STRING
) COMMENT '���ģ�û���Ϊ���ݼ�'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/bigdatacase/dataset';

-- ��֤���ݵ��루��ѯǰ15�У�
SELECT * FROM raw_user LIMIT 20;
"

echo "===== Ԥ����ȫ������� ====="
echo "? Ԥ�����ļ���${DATASET_DIR}/raw_user_table.txt"
echo "? HDFS·����/bigdatacase/dataset/raw_user_table.txt"