#!/bin/bash

# 定义基础路径（使用用户可读写目录）
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"
MYSQL_PWD="hive"  # MySQL hive用户密码
OUTPUT_DIR="/home/hadoop/bigdata-output"  # 改为用户主目录下的路径
HBASE_TMP_DIR="/user/hbase/bigdata_tmp"
HDFS_INPUT_DIR="/user/hbase/bigdata_input"
HDFS_OUTPUT_DIR="/user/hbase/bigdata_output_hfile"

# 创建输出目录（用户主目录无需管理员权限）
mkdir -p ${OUTPUT_DIR}

# 1. 启动服务
echo "===== 启动服务 ====="

# 启动MySQL（自动输入密码，需确保mysql命令支持--password=选项）
echo "--- 启动MySQL服务 ---"
if command -v systemctl &>/dev/null; then
    systemctl start mysql
else
    service mysql start
fi

# 启动Hadoop（hadoop用户有权限）
echo "--- 启动Hadoop服务 ---"
cd ${HADOOP_DIR}
./sbin/start-dfs.sh
./sbin/start-yarn.sh

# 启动HBase
echo "--- 启动HBase服务 ---"
${HBASE_HOME}/bin/start-hbase.sh


# 2. Hive数据导出到本地
echo "===== Hive数据导出 ====="

# 进入Hive执行操作（明确使用hive用户密码）
echo "--- 在Hive中创建临时表并导出数据 ---"
${HIVE_DIR}/bin/hive -e "
-- 使用dblab数据库
USE dblab;

-- 创建大数据集临时表
CREATE TABLE IF NOT EXISTS raw_user_action (
    id STRING,
    uid STRING,
    item_id STRING,
    behavior_type STRING,
    item_category STRING,
    visit_date DATE,
    province STRING
) COMMENT '大数据集临时表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- 从raw_user表导入数据到临时表
INSERT OVERWRITE TABLE raw_user_action SELECT * FROM raw_user;

-- 导出数据到本地（用户主目录路径）
SET mapred.reduce.tasks=1;
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}/bigdata-user-table'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
SELECT * FROM dblab.raw_user_action;
"

# 验证导出结果
echo "--- 查看导出数据前10行 ---"
head ${OUTPUT_DIR}/bigdata-user-table/000000_0


# 3. 导入数据到MySQL（明确密码参数）
echo "===== 数据导入MySQL ====="

# 登录MySQL并创建表
echo "--- 创建MySQL表并导入数据 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
-- 使用dblab数据库
USE dblab;

-- 创建大数据集表
CREATE TABLE IF NOT EXISTS raw_user_action (
    id VARCHAR(50),
    uid VARCHAR(50),
    item_id VARCHAR(50),
    behavior_type VARCHAR(50),
    item_category VARCHAR(50),
    visit_date DATE,
    province VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 清空表
TRUNCATE TABLE raw_user_action;

-- 导入数据（用户主目录路径）
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

# 验证MySQL数据
echo "--- 查看MySQL表前10条记录 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SELECT * FROM raw_user_action LIMIT 10;
"


# 4. 从MySQL导出并导入HBase
echo "===== 数据导入HBase ====="

# 从MySQL导出数据到本地（自动传递密码）
echo "--- 从MySQL导出数据 ---"
mysql -u hive --password=${MYSQL_PWD} -e "SELECT * FROM dblab.raw_user_action" > ${OUTPUT_DIR}/raw_user_action.tsv

# 删除表头
sed -i '1d' ${OUTPUT_DIR}/raw_user_action.tsv

# 上传到HDFS（hadoop用户有权限）
echo "--- 上传数据到HDFS ---"
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR}
hdfs dfs -put ${OUTPUT_DIR}/raw_user_action.tsv ${HDFS_INPUT_DIR}

# 在HBase中创建表
echo "--- 在HBase中创建表 ---"
${HBASE_HOME}/bin/hbase shell <<EOF
create 'raw_user_action', {NAME => 'f1', VERSIONS => 5}
exit
EOF

# 创建HBase临时目录（HDFS操作由hadoop用户执行）
echo "--- 创建HBase临时目录 ---"
hdfs dfs -mkdir -p ${HBASE_TMP_DIR}
hdfs dfs -chown hadoop:hadoop ${HBASE_TMP_DIR}

# 删除旧的HFile输出
echo "--- 清理旧数据 ---"
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR} 2>/dev/null

# 使用ImportTsv生成HFile
echo "--- 生成HFile ---"
${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dhbase.fs.tmp.dir=file://${HBASE_TMP_DIR} \
  -Dhbase.rootdir=file://${HBASE_TMP_DIR} \
  -Dimporttsv.separator=9 \
  -Dimporttsv.columns="HBASE_ROW_KEY,f1:uid,f1:item_id,f1:behavior_type,f1:item_category,f1:visit_date,f1:province" \
  -Dimporttsv.bulk.output=hdfs://localhost:9000${HDFS_OUTPUT_DIR} \
  raw_user_action \
  hdfs://localhost:9000${HDFS_INPUT_DIR}/raw_user_action.tsv

# 将HFile加载到HBase
echo "--- 加载HFile到HBase ---"
${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
  hdfs://localhost:9000${HDFS_OUTPUT_DIR} \
  raw_user_action

# 验证HBase数据
echo "--- 查看HBase表前5条记录 ---"
${HBASE_HOME}/bin/hbase shell <<EOF
scan 'raw_user_action', {LIMIT => 5}
exit
EOF

echo "===== 大数据集数据互导完成 ====="
echo "Hive表: dblab.raw_user_action"
echo "MySQL表: dblab.raw_user_action (用户:hive, 数据库:dblab)"
echo "HBase表: raw_user_action"