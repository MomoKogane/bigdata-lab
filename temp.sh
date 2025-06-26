#!/bin/bash

# ===== 配置参数 =====
# 基础路径
HADOOP_DIR="/usr/local/hadoop"
HIVE_DIR="/usr/local/hive"
HBASE_HOME="/usr/local/hbase"
MYSQL_PWD="hive"

# 输出目录（使用绝对路径）
OUTPUT_DIR="/usr/local/output/bigdata-user-table"

# HDFS目录 - 使用实际主机名替代"hdfs"
HDFS_HOST=$(hdfs getconf -confKey fs.defaultFS | sed 's|^hdfs://||; s|:.*||')
HBASE_TMP_DIR="/user/hbase/bigdata_tmp"
HDFS_INPUT_DIR="/user/hbase/bigdata_input"
HDFS_OUTPUT_DIR="/user/hbase/bigdata_output_hfile"

# 大数据集分块参数
BLOCK_SIZE=500000  # MySQL每块50万条
HIVE_REDUCERS=20   # 根据集群规模调整

# 日志文件
LOG_FILE="${OUTPUT_DIR}/migration_$(date +%Y%m%d_%H%M%S).log"

# ===== 日志函数 =====
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# ===== 初始化 =====
log "===== 初始化环境 ====="

# 创建输出目录
sudo mkdir -p ${OUTPUT_DIR} 2>/dev/null
sudo chown -R $USER:$USER ${OUTPUT_DIR} 2>/dev/null
mkdir -p ${OUTPUT_DIR} 2>/dev/null

# 清理旧数据
log "--- 清理旧输出 ---"
rm -f ${OUTPUT_DIR}/* 2>/dev/null
hdfs dfs -rm -r ${HDFS_INPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -rm -r ${HBASE_TMP_DIR} >> "$LOG_FILE" 2>&1

# 确保HDFS目录存在
hdfs dfs -mkdir -p ${HDFS_INPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -mkdir -p ${HDFS_OUTPUT_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -mkdir -p ${HBASE_TMP_DIR} >> "$LOG_FILE" 2>&1
hdfs dfs -chown hadoop:hadoop ${HBASE_TMP_DIR} >> "$LOG_FILE" 2>&1

# ===== 1. 启动服务 =====
log "===== 启动服务 ====="

# 检查并启动MySQL
log "--- 检查并启动MySQL服务 ---"
if ! mysqladmin ping -u hive -p${MYSQL_PWD} >/dev/null 2>&1; then
    if command -v systemctl &>/dev/null; then
        sudo systemctl start mysql >> "$LOG_FILE" 2>&1
    else
        sudo service mysql start >> "$LOG_FILE" 2>&1
    fi
    sleep 5  # 等待服务启动
    
    # 验证启动
    if ! mysqladmin ping -u hive -p${MYSQL_PWD} >/dev/null 2>&1; then
        log "!!! MySQL服务启动失败，退出脚本 !!!"
        exit 1
    else
        log "MySQL服务启动成功"
    fi
else
    log "MySQL服务已运行"
fi

# 检查并启动Hadoop
log "--- 检查并启动Hadoop服务 ---"
if ! jps | grep -q NameNode || ! jps | grep -q ResourceManager; then
    cd ${HADOOP_DIR}
    ./sbin/start-dfs.sh >> "$LOG_FILE" 2>&1
    ./sbin/start-yarn.sh >> "$LOG_FILE" 2>&1
    
    # 等待服务稳定
    sleep 10
    
    # 验证启动
    if ! jps | grep -q NameNode || ! jps | grep -q ResourceManager; then
        log "!!! Hadoop服务启动失败，退出脚本 !!!"
        jps >> "$LOG_FILE" 2>&1
        exit 1
    else
        log "Hadoop服务启动成功"
    fi
else
    log "Hadoop服务已运行"
fi

# 检查并启动HBase
log "--- 检查并启动HBase服务 ---"
if ! jps | grep -q HMaster; then
    ${HBASE_HOME}/bin/start-hbase.sh >> "$LOG_FILE" 2>&1
    sleep 5
    
    # 验证启动
    if ! jps | grep -q HMaster; then
        log "!!! HBase服务启动失败，退出脚本 !!!"
        jps >> "$LOG_FILE" 2>&1
        exit 1
    else
        log "HBase服务启动成功"
    fi
else
    log "HBase服务已运行"
fi

# ===== 2. Hive数据导出 =====
log "===== Hive数据导出 ====="

# 创建临时表并导出数据（大数据集优化）
log "--- 在Hive中创建临时表并导出数据 ---"
${HIVE_DIR}/bin/hive -e "
-- 性能优化配置
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=5000;
SET hive.optimize.sort.dynamic.partition=true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

USE dblab;

-- 删除旧临时表（如果存在）
DROP TABLE IF EXISTS raw_user_action;

-- 创建临时表（ORC格式提升性能）
CREATE TABLE raw_user_action (
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
" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "!!! Hive数据导出失败 !!!"
    exit 1
fi

# 验证导出结果
log "--- 查看导出文件数量 ---"
ls ${OUTPUT_DIR} | wc -l | tee -a "$LOG_FILE"
log "--- 查看导出数据前5行 ---"
find ${OUTPUT_DIR} -type f -exec head -n 5 {} \; | tee -a "$LOG_FILE"

# ===== 3. 导入数据到MySQL =====
log "===== 数据导入MySQL ====="

# 登录MySQL并创建表（保持与原始结构一致）
log "--- 创建/重建MySQL表结构 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;

-- 删除旧表（如果存在）
DROP TABLE IF EXISTS raw_user_action;

-- 创建表（与原始结构完全一致）
CREATE TABLE raw_user_action (
    id VARCHAR(50),
    uid VARCHAR(50),
    item_id VARCHAR(50),
    behavior_type VARCHAR(50),
    item_category VARCHAR(50),
    visit_date DATE,
    province VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 禁用检查加速导入
SET unique_checks=0;
SET foreign_key_checks=0;
" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "!!! MySQL表创建失败 !!!"
    exit 1
fi

# 分块导入数据（大数据集优化）
log "--- 分块导入数据 ---"
files=(${OUTPUT_DIR}/00*)
file_count=${#files[@]}
counter=0

for file in "${files[@]}"; do
    counter=$((counter+1))
    log "导入文件块 [$counter/$file_count]: $(basename $file)"
    
    # 导入前处理日期格式
    awk -F',' 'BEGIN {OFS=","} {
        # 处理日期格式：确保是YYYY-MM-DD
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
        log "!!! 文件导入失败: $(basename $file) !!!"
    else
        rm "${file}_processed"
    fi
    
    # 每导入5个文件显示进度
    if [ $((counter % 5)) -eq 0 ]; then
        mysql -u hive --password=${MYSQL_PWD} -e "USE dblab; SELECT COUNT(*) AS total_rows FROM raw_user_action;" | tee -a "$LOG_FILE"
    fi
done

# 重新启用检查
log "--- 启用完整性检查 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SET unique_checks=1;
SET foreign_key_checks=1;
" >> "$LOG_FILE" 2>&1

# 最终验证
log "--- MySQL表统计 ---"
mysql -u hive --password=${MYSQL_PWD} -e "
USE dblab;
SELECT COUNT(*) AS total_rows FROM raw_user_action;
SELECT MIN(visit_date) AS min_date, MAX(visit_date) AS max_date FROM raw_user_action;
" | tee -a "$LOG_FILE"

# ===== 4. 导入数据到HBase =====
log "===== 数据导入HBase ====="

# 从MySQL导出数据到本地（分块处理）
log "--- 从MySQL分块导出数据 ---"
total_rows=$(mysql -u hive --password=${MYSQL_PWD} -sN -e "USE dblab; SELECT COUNT(*) FROM raw_user_action;" 2>>"$LOG_FILE")
blocks=$(( (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE ))
log "总行数: $total_rows, 分块数: $blocks"

for ((i=0; i<blocks; i++)); do
    offset=$((i * BLOCK_SIZE))
    log "导出批次 [$((i+1))/$blocks]: 行 $offset - $((offset + BLOCK_SIZE))"
    
    mysql -u hive --password=${MYSQL_PWD} -e "
    USE dblab;
    SELECT * FROM raw_user_action 
    LIMIT ${BLOCK_SIZE} OFFSET ${offset}
    " > ${OUTPUT_DIR}/raw_user_action_${i}.tsv 2>>"$LOG_FILE"
    
    # 检查文件是否为空
    if [ ! -s "${OUTPUT_DIR}/raw_user_action_${i}.tsv" ]; then
        log "!!! 导出文件为空: raw_user_action_${i}.tsv !!!"
    fi
done

# 上传到HDFS
log "--- 上传数据到HDFS ---"
hdfs dfs -put ${OUTPUT_DIR}/raw_user_action_*.tsv ${HDFS_INPUT_DIR}/ >> "$LOG_FILE" 2>&1

# 修复HBase表操作
log "--- 在HBase中创建/重建表 ---"
${HBASE_HOME}/bin/hbase shell <<EOF 2>>"$LOG_FILE"
# 检查表是否存在，如果存在则禁用并删除
if exists 'raw_user_action'
    disable 'raw_user_action'
    drop 'raw_user_action'
end
# 创建新表
create 'raw_user_action', 
  {NAME => 'f1', VERSIONS => 5}, 
  {SPLITS => ['1000000', '3000000', '5000000', '7000000', '9000000']}
EOF

if [ $? -ne 0 ]; then
    log "!!! HBase表创建失败 !!!"
    exit 1
fi

# 分批生成HFile - 使用实际主机名
log "--- 分批生成HFile ---"
# 确保HFile输出目录不存在（ImportTsv要求目录不存在）
hdfs dfs -rm -r ${HDFS_OUTPUT_DIR}/* >> "$LOG_FILE" 2>&1

for file in $(hdfs dfs -ls ${HDFS_INPUT_DIR} | grep tsv | awk '{print $NF}'); do
    log "处理文件: $file"
    # 为每个文件创建独立的HFile输出目录
    hfile_output="${HDFS_OUTPUT_DIR}/$(basename ${file} .tsv)_hfiles"
    # 删除可能存在的旧目录
    hdfs dfs -rm -r ${hfile_output} >> "$LOG_FILE" 2>&1

    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
      -Dimporttsv.rowkey.position=0 \
      -Dimporttsv.separator=9 \
      -Dimporttsv.columns="HBASE_ROW_KEY,f1:uid,f1:item_id,f1:behavior_type,f1:item_category,f1:visit_date,f1:province" \
      -Dimporttsv.bulk.output="${hfile_output}" \
      raw_user_action \
      ${file} >> "$LOG_FILE" 2>&1

    if [ $? -ne 0 ]; then
        log "!!! ImportTsv失败: $(basename $file) !!!"
        # 记录失败，但继续处理其他文件
    else
        log "生成HFile成功: ${hfile_output}"
    fi
done

# 分批加载HFile
log "--- 分批加载HFile到HBase ---"
for hfile_dir in $(hdfs dfs -ls ${HDFS_OUTPUT_DIR} | grep hfiles | awk '{print $NF}'); do
    log "加载HFile: $hfile_dir"
    
    ${HBASE_HOME}/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
      ${hfile_dir} \
      raw_user_action >> "$LOG_FILE" 2>&1
    
    if [ $? -ne 0 ]; then
        log "!!! LoadIncrementalHFiles失败: $(basename $hfile_dir) !!!"
    fi
done

# 最终验证
log "--- HBase表验证 ---"
${HBASE_HOME}/bin/hbase shell <<EOF 2>>"$LOG_FILE"
count 'raw_user_action', INTERVAL => 1000000
exit
EOF

log "===== 大数据集数据互导完成 ====="
log "Hive表: dblab.raw_user_action"
log "MySQL表: dblab.raw_user_action (用户:hive, 数据库:dblab)"
log "HBase表: raw_user_action"
log "输出目录: ${OUTPUT_DIR}"
log "完整日志: ${LOG_FILE}"