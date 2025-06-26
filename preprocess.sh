#!/bin/bash

# �������·��
DATASET_DIR="/usr/local/bigdatacase/dataset"
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"

# 1. ����pre_deal.sh����ԭʼ����
echo "===== ��ʼ����Ԥ���� ====="
cd ${DATASET_DIR}
echo "--- ִ��pre_deal.sh����raw_user.csv ---"
if bash ./pre_deal.sh raw_user.csv raw_user_table.txt; then
    echo "--- pre_deal.shִ�гɹ� ---"
else
    echo "--- pre_deal.shִ��ʧ�ܣ��˳�Ԥ����ű� ---"
    exit 1
fi

# ��֤Ԥ���������鿴ǰ10�У�
echo "--- Ԥ���������Ԥ�� ---"
head -10 raw_user_table.txt


# 2. �ϴ���HDFS����֤
echo "===== ��ʼHDFS���� ====="
cd ${HADOOP_DIR}

# ����Hadoop
echo "--- ��鲢����Hadoop ---"
if jps | grep -q NameNode; then
    echo "--- Hadoop�Ѿ������� ---"
else
    echo "--- ����Hadoop ---"
    ./sbin/start-all.sh
fi

# �ϴ��ļ���HDFS
echo "--- �ϴ�raw_user_table.txt��HDFS ---"
if ./bin/hdfs dfs -put ${DATASET_DIR}/raw_user_table.txt /bigdatacase/dataset; then
    echo "--- �ļ��ϴ��ɹ� ---"
else
    echo "--- �ļ��ϴ�ʧ�ܣ��˳�Ԥ����ű� ---"
    exit 1
fi

# �鿴ǰ15��
echo "--- HDFS�ļ�����Ԥ�� ---"
./bin/hdfs dfs -cat /bigdatacase/dataset/raw_user_table.txt | head -15


# 3. ��Hive�д����ⲿ����֤
echo "===== ��ʼHive���� ====="

# ����MySQL
echo "--- ��鲢����MySQL ---"
if service mysql status; then
    echo "--- MySQL�Ѿ������� ---"
else
    echo "--- ����MySQL ---"
    service mysql start
fi

# ����Hiveִ�н���Ͳ�ѯ
echo "--- ��Hive�д����ⲿ����֤���� ---"
hive -e "
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