#!/bin/bash

# ===== 配置参数 =====
# 基础路径
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"
HBASE_HOME="/usr/local/hbase"
MYSQL_PWD="hive"

# 输出目录（使用绝对路径）
OUTPUT_DIR="/usr/local/output/bigdata-user-table"

# HDFS目录
HBASE_TMP_DIR="/user/hbase/bigdata_tmp"
HDFS_INPUT_DIR="/user/hbase/bigdata_input"
HDFS_OUTPUT_DIR="/user/hbase/bigdata_output_hfile"

# 大数据集分块参数
BLOCK_SIZE=500000  # MySQL每块50万条
HIVE_REDUCERS=20   # 根据集群规模调整

# ===== 初始化 =====
echo "===== 初始化环境 ====="
# 创建输出目录（需要sudo权限）
sudo mkdir -p ${OUTPUT_DIR}
sudo chown -R $USER:$USER ${OUTPUT_DIR}

# 清理旧数据
echo "--- 清理旧输出 ---"
rm -f ${OUTPUT_DIR}/* 2>/dev/null
hdfs dfs -rm -r ${HDFS_INPUT_DIR} 2>/dev/null
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR} 2>/dev/null
hdfs dfs -rm -r ${HBASE_TMP_DIR} 2>/dev/null

# ===== 1. 启动服务 =====
echo "===== 启动服务 ====="

# 启动MySQL
echo "--- 启动MySQL服务 ---"
if command -v systemctl &>/dev/null; then
    sudo systemctl start mysql
else
    sudo service mysql start
fi
sleep 5  # 等待服务启动

# 启动Hadoop
echo "--- 启动Hadoop服务 ---"
cd ${HADOOP_DIR}
./sbin/start-dfs.sh
./sbin/start-yarn.sh
if [ $? -ne 0 ]; then
    echo "--- Hadoop服务启动失败，退出脚本 ---"
    exit 1
fi
sleep 10  # 等待服务稳定

# 启动HBase
echo "--- 启动HBase服务 ---"
${HBASE_HOME}/bin/start-hbase.sh
if [ $? -ne 0 ]; then
    echo "--- HBase服务启动失败，退出脚本 ---"
    exit 1
fi
sleep 5

# ===== 2. Hive数据导出 =====
echo "===== Hive数据导出 ====="

# 创建临时表并导出数据（大数据集优化）
echo "--- 在Hive中创建临时表并导出数据 ---"
${HIVE_DIR}/bin/hive -e "
-- 性能优化配置
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.optimize.sort.dynamic.partition=true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

USE dblab;

-- 创建临时表（ORC格式提升性能）
CREATE TABLE IF NOT EXISTS raw_user_action (
    id STRING,
    uid STRING,
    item_id STRING,
    behavior_type STRING,
    item_category STRING,
    visit_date DATE,
    province STRING
) COMMENT '大数据集临时表'
STORED AS ORC;

-- 从raw_user表导入数据（并行处理）
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
INSERT OVERWRITE TABLE raw_user_action SELECT * FROM raw_user;

-- 导出数据到本地（多文件输出）
SET mapred.reduce.tasks=${HIVE_REDUCERS};
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
SELECT * FROM raw_user_action;
"

# 验证导出结果
echo "--- 查看导出文件数量 ---"
ls ${OUTPUT_DIR} | wc -l
echo "--- 查看导出数据前5行 ---"
find ${OUTPUT_DIR} -type f -print0 | xargs -0 head -n 5

# ===== 3. 导入数据到MySQL =====
echo "===== 数据导入MySQL ====="

# 登录MySQL并创建表（保持与原始结构一致）
echo "--- 创建MySQL表结构 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;

-- 创建表（与原始结构完全一致）
CREATE TABLE IF NOT EXISTS raw_user_action (
    id VARCHAR(50),
    uid VARCHAR(50),
    item_id VARCHAR(50),
    behavior_type VARCHAR(50),
    item_category VARCHAR(50),
    visit_date DATE,  -- 确保DATE类型
    province VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

TRUNCATE TABLE raw_user_action;

-- 禁用检查加速导入
SET unique_checks=0;
SET foreign_key_checks=0;
"

# 分块导入数据（大数据集优化）
echo "--- 分块导入数据 ---"
file_count=$(ls ${OUTPUT_DIR}/00* | wc -l)
counter=0

for file in $(ls ${OUTPUT_DIR}/00*); do
    counter=$((counter+1))
    echo "导入文件块 [$counter/$file_count]: $(basename $file)"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    LOAD DATA LOCAL INFILE '${file}'
    INTO TABLE raw_user_action
    CHARACTER SET utf8
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    (id, uid, item_id, behavior_type, item_category, @date_var, province)
    SET visit_date = STR_TO_DATE(@date_var, '%Y-%m-%d');  -- 确保日期格式正确
    "
    
    # 每导入5个文件显示进度
    if [ $((counter % 5)) -eq 0 ]; then
        mysql -u hive --password=${MYSQL_PWD} -e "USE dblab; SELECT COUNT(*) AS total_rows FROM raw_user_action;"
    fi
done

# 重新启用检查
echo "--- 启用完整性检查 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SET unique_checks=1;
SET foreign_key_checks=1;
"

# 最终验证
echo "--- MySQL表统计 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SELECT COUNT(*) AS total_rows FROM raw_user_action;
SELECT MIN(visit_date) AS min_date, MAX(visit_date) AS max_date FROM raw_user_action;
"

# ===== 4. 导入数据到HBase =====
echo "===== 数据导入HBase ====="

# 从MySQL导出数据到本地（分块处理）
echo "--- 从MySQL分块导出数据 ---"
total_rows=$(mysql -u hive --password=${MYSQL_PWD} -sN -e "USE dblab; SELECT COUNT(*) FROM raw_user_action;")
blocks=$(( (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE ))

for ((i=0; i<blocks; i++)); do
    offset=$((i * BLOCK_SIZE))
    echo "导出批次 [$((i+1))/$blocks]: 行 $offset - $((offset + BLOCK_SIZE))"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    SELECT * FROM raw_user_action 
    LIMIT ${BLOCK_SIZE} OFFSET ${offset}
    " > ${OUTPUT_DIR}/raw_user_action_${i}.tsv
done

# 上传到HDFS
echo "--- 上传数据到HDFS ---"
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR}
hdfs dfs -put ${OUTPUT_DIR}/raw_user_action_*.tsv ${HDFS_INPUT_DIR}/

# 在HBase中创建表（带预分区）
echo "--- 在HBase中创建表 ---"
${HBASE_HOME}/bin/hbase shell <<EOF
disable 'raw_user_action'
drop 'raw_user_action'
create 'raw_user_action', 
  {NAME => 'f1', VERSIONS => 5}, 
  {SPLITS => ['1000000', '3000000', '5000000', '7000000', '9000000']}  # 预分区
EOF

# 创建HBase临时目录
echo "--- 创建HBase临时目录 ---"
hdfs dfs -mkdir -p ${HBASE_TMP_DIR}
hdfs dfs -chown hadoop:hadoop ${HBASE_TMP_DIR}

# 分批生成HFile
echo "--- 分批生成HFile ---"
for file in $(hdfs dfs -ls ${HDFS_INPUT_DIR} | grep tsv | awk '{print $NF}'); do
    echo "处理文件: $file"
    
    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
      -Dimporttsv.rowkey.position=0 \
      -Dimporttsv.separator=9 \
      -Dimporttsv.columns="HBASE_ROW_KEY,f1:uid,f1:item_id,f1:behavior_type,f1:item_category,f1:visit_date,f1:province" \
      -Dimporttsv.bulk.output=hdfs://$(hdfs getconf -confKey fs.defaultFS)${HDFS_OUTPUT_DIR}/$(basename ${file})_hfiles \
      raw_user_action \
      ${file}
done

# 分批加载HFile
echo "--- 分批加载HFile到HBase ---"
for hfile_dir in $(hdfs dfs -ls ${HDFS_OUTPUT_DIR} | grep hfiles | awk '{print $NF}'); do
    echo "加载HFile: $hfile_dir"
    
    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
      ${hfile_dir} \
      raw_user_action
done

# 最终验证
echo "--- HBase表验证 ---"
${HBASE_HOME}/bin/hbase shell <<EOF
count 'raw_user_action', INTERVAL => 1000000
exit
EOF

echo "===== 大数据集数据互导完成 ====="
echo "Hive表: dblab.raw_user_action"
echo "MySQL表: dblab.raw_user_action (用户:hive, 数据库:dblab)"
echo "HBase表: raw_user_action"
echo "输出目录: ${OUTPUT_DIR}"