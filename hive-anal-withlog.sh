#!/bin/bash

# 定义服务路径和日志文件
MYSQL_SERVICE="mysql"
LOG_FILE="hive_analysis_$(date +%Y%m%d_%H%M%S).log"
RESULTS_FILE="analysis_results.txt"

# 初始化结果文件
echo "===== Hive 分析结果报告 =====" > "$RESULTS_FILE"
echo "生成时间: $(date)" >> "$RESULTS_FILE"
echo "============================" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# 日志记录函数
log() {
    echo "$1" | tee -a "$LOG_FILE"
}

# 结果记录函数
record_result() {
    echo "$1" | tee -a "$RESULTS_FILE" "$LOG_FILE"
}

# 启动Hadoop服务
start_hadoop() {
    log "===== 检测并启动Hadoop服务 ====="
    local namenode_running=$(jps | grep -c NameNode)
    local resourcemanager_running=$(jps | grep -c ResourceManager)
    
    if [ ${namenode_running} -eq 0 ] || [ ${resourcemanager_running} -eq 0 ]; then
        log "--- 启动Hadoop集群 ---"
        start-all.sh >> "$LOG_FILE" 2>&1
        
        # 等待服务启动（最多30秒）
        log "--- 等待Hadoop服务启动 ---"
        for i in {1..30}; do
            local nn=$(jps | grep -c NameNode)
            local rm=$(jps | grep -c ResourceManager)
            if [ ${nn} -gt 0 ] && [ ${rm} -gt 0 ]; then
                log "Hadoop服务启动成功"
                break
            fi
            sleep 1
        done
        
        # 检查最终状态
        if [ ${nn} -eq 0 ] || [ ${rm} -eq 0 ]; then
            log "Hadoop服务启动失败，请手动检查"
            exit 1
        fi
    else
        log "Hadoop服务已运行"
    fi
    log "--------------------------------------------------"
}

# 启动MySQL服务
start_mysql() {
    log "===== 检测并启动MySQL服务 ====="
    if ! systemctl status ${MYSQL_SERVICE} &>/dev/null; then
        if command -v service &>/dev/null; then
            log "--- 使用service启动MySQL ---"
            service ${MYSQL_SERVICE} start >> "$LOG_FILE" 2>&1
        elif command -v systemctl &>/dev/null; then
            log "--- 使用systemctl启动MySQL ---"
            systemctl start ${MYSQL_SERVICE} >> "$LOG_FILE" 2>&1
        else
            log "无法检测MySQL启动方式，请手动启动"
            exit 1
        fi
        
        # 等待服务启动
        log "--- 等待MySQL服务启动 ---"
        for i in {1..10}; do
            if mysqladmin ping -h localhost -u root -p &>/dev/null; then
                log "MySQL服务启动成功"
                break
            fi
            sleep 1
        done
        
        # 检查最终状态
        if ! mysqladmin ping -h localhost -u root -p &>/dev/null; then
            log "MySQL服务启动失败，请手动检查"
            exit 1
        fi
    else
        log "MySQL服务已运行"
    fi
    log "--------------------------------------------------"
}

# 定义Hive执行命令函数
execute_hive() {
    local hive_command="$1"
    log "===== 执行Hive命令: $hive_command ====="
    log "===== 使用dblab数据库 ======"
    
    # 执行Hive命令并将输出重定向到日志文件
    if hive -e "USE dblab; $hive_command" >> "$LOG_FILE" 2>&1; then
        log "--- Hive命令执行成功 ---"
    else
        log "--- Hive命令执行失败，退出预处理脚本 ---"
        exit 1
    fi
    log "--------------------------------------------------"
}

# 执行Hive查询并捕获数值结果
execute_and_capture() {
    local description="$1"
    local query="$2"
    local result_var="$3"
    
    log "===== 执行查询: $description ====="
    log "查询: $query"
    
    # 执行查询并捕获结果
    local result
    result=$(hive -S -e "USE dblab; $query" 2>> "$LOG_FILE")
    
    if [ $? -ne 0 ]; then
        log "查询执行失败"
        exit 1
    fi
    
    # 记录结果
    record_result "? $description: $result"
    eval "$result_var=\"$result\""
    log "查询结果: $result"
    log "--------------------------------------------------"
}

# 主流程
{
    start_hadoop
    start_mysql

    # 1. 查看数据库和表信息
    execute_hive "
    -- 查看所有数据库
    show databases;

    -- 使用dblab数据库
    use dblab;

    -- 查看数据库中的表
    show tables;

    -- 查看raw_user表结构
    describe raw_user;

    -- 查看表创建语句
    show create table raw_user;
    "

    # 2. 简单查询分析
    execute_hive "
    -- 查看前10个用户的行为类型
    select behavior_type from raw_user limit 10;

    -- 查询前20位用户购买商品的时间和商品种类（behavior_type=4表示购买）
    select visit_date, item_category 
    from raw_user 
    where behavior_type=4 
    limit 20;

    -- 利用嵌套语句查询前20条记录的商品种类和操作类别
    select e.bh, e.it 
    from (
        select behavior_type as bh, item_category as it 
        from raw_user
    ) as e 
    limit 20;
    "

    # 3. 查询统计分析 - 捕获数值结果
    record_result "===== 关键指标分析 ====="
    
    local total_rows distinct_users unique_records
    execute_and_capture "数据集总行数" "select count(*) from raw_user;" total_rows
    execute_and_capture "不重复用户数" "select count(distinct uid) from raw_user;" distinct_users
    execute_and_capture "完全唯一的用户行为记录数量" \
        "select count(*) from (select uid, item_id, behavior_type, item_category, visit_date, province from raw_user group by uid, item_id, behavior_type, item_category, visit_date, province having count(*)=1) a;" \
        unique_records
    
    # 4. 计算用户转化漏斗 - 捕获详细结果
    record_result "===== 用户转化漏斗 ====="
    log "===== 计算用户转化漏斗 ====="
    
    # 执行漏斗查询并将完整结果保存到结果文件
    hive -e "USE dblab;
        WITH stage_users AS (
            SELECT 
                CASE behavior_type
                    WHEN 1 THEN '浏览'
                    WHEN 2 THEN '加入购物车'
                    WHEN 3 THEN '收藏'
                    WHEN 4 THEN '购买'
                END AS stage,
                COUNT(DISTINCT uid) AS users
            FROM raw_user
            WHERE behavior_type IN (1, 2, 3, 4)
            GROUP BY behavior_type
        ),
        funnel_data AS (
            SELECT 
                stage,
                users,
                FIRST_VALUE(users) OVER (ORDER BY CASE stage 
                    WHEN '浏览' THEN 1 
                    WHEN '加入购物车' THEN 2 
                    WHEN '收藏' THEN 3 
                    WHEN '购买' THEN 4 
                END) AS browse_users
            FROM stage_users
        )
        SELECT 
            stage,
            users,
            CONCAT(ROUND(users * 100.0 / browse_users, 2), '%') AS conversion_rate
        FROM funnel_data
        ORDER BY CASE stage 
            WHEN '浏览' THEN 1 
            WHEN '加入购物车' THEN 2 
            WHEN '收藏' THEN 3 
            WHEN '购买' THEN 4 
        END;" | tee -a "$RESULTS_FILE" >> "$LOG_FILE"
    
    if [ $? -ne 0 ]; then
        log "转化漏斗查询失败"
        exit 1
    fi
    
    log "转化漏斗查询完成"
    log "--------------------------------------------------"

    # 汇总报告
    record_result ""
    record_result "===== 分析报告摘要 ====="
    record_result "1. 总数据行数: $total_rows"
    record_result "2. 独立用户数: $distinct_users"
    record_result "3. 唯一行为记录数: $unique_records"
    record_result "4. 转化漏斗详情见上方表格"
    record_result ""
    record_result "详细日志: $LOG_FILE"
    record_result "生成时间: $(date)"

    log "===== 大规模数据集Hive分析完成 ====="
    log "分析内容：数据库查看、基础查询、统计分析、转化漏斗计算"
    log "数据来源：dblab.raw_user（大规模用户行为数据集）"
    log "关键结果已保存至: $RESULTS_FILE"
    log "详细日志已保存至: $LOG_FILE"
} | tee -a "$LOG_FILE"  # 确保所有输出都记录到日志