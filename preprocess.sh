#!/bin/bash

# 定义基础路径
DATASET_DIR="/usr/local/bigdatacase/dataset"
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"

# 1. 调用pre_deal.sh处理原始数据
echo "===== 开始数据预处理 ====="
cd ${DATASET_DIR}
echo "--- 执行pre_deal.sh处理raw_user.csv ---"
bash ./pre_deal.sh raw_user.csv raw_user_table.txt

# 验证预处理结果（查看前10行）
echo "--- 预处理后数据预览 ---"
head -10 raw_user_table.txt


# 2. 上传到HDFS并验证
echo "===== 开始HDFS操作 ====="
cd ${HADOOP_DIR}

# 启动Hadoop
echo "--- 检查并启动Hadoop ---"
jps | grep -q NameNode || ./sbin/start-all.sh

# 上传文件到HDFS
echo "--- 上传raw_user_table.txt到HDFS ---"
./bin/hdfs dfs -put ${DATASET_DIR}/raw_user_table.txt /bigdatacase/dataset

# 查看前15行
echo "--- HDFS文件内容预览 ---"
./bin/hdfs dfs -cat /bigdatacase/dataset/raw_user_table.txt | head -15


# 3. 在Hive中创建外部表并验证
echo "===== 开始Hive操作 ====="

# 启动MySQL
echo "--- 检查并启动MySQL ---"
service mysql status || service mysql start

# 进入Hive执行建表和查询
cd ${HIVE_DIR}
echo "--- 在Hive中创建外部表并验证数据 ---"
./bin/hive -e "
USE dblab;

-- 创建外部表
CREATE EXTERNAL TABLE IF NOT EXISTS raw_user (
    id INT,
    uid STRING,
    item_id STRING,
    behavior_type INT,
    item_category STRING,
    visit_date DATE,
    province STRING
) COMMENT '大规模用户行为数据集'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/bigdatacase/dataset';

-- 验证数据导入（查询前15行）
SELECT * FROM raw_user LIMIT 20;
"

echo "===== 预处理全流程完成 ====="
echo "? 预处理文件：${DATASET_DIR}/raw_user_table.txt"
echo "? HDFS路径：/bigdatacase/dataset/raw_user_table.txt"