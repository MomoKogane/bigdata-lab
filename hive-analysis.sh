#!/bin/bash

# 定义服务路径

MYSQL_SERVICE="mysql"

# 启动Hadoop服务
start_hadoop() {
    echo "===== 检测并启动Hadoop服务 ====="
    local namenode_running=$(jps | grep -c NameNode)
    local resourcemanager_running=$(jps | grep -c ResourceManager)
    
    if [ ${namenode_running} -eq 0 ] || [ ${resourcemanager_running} -eq 0 ]; then
        echo "--- 启动Hadoop集群 ---"
        start-all.sh
        
        # 等待服务启动（最多30秒）
        echo "--- 等待Hadoop服务启动 ---"
        for i in {1..30}; do
            local nn=$(jps | grep -c NameNode)
            local rm=$(jps | grep -c ResourceManager)
            if [ ${nn} -gt 0 ] && [ ${rm} -gt 0 ]; then
                echo "Hadoop服务启动成功"
                break
            fi
            sleep 1
        done
        
        # 检查最终状态
        if [ ${nn} -eq 0 ] || [ ${rm} -eq 0 ]; then
            echo "Hadoop服务启动失败，请手动检查"
            exit 1
        fi
    else
        echo "Hadoop服务已运行"
    fi
    echo "--------------------------------------------------"
}

# 启动MySQL服务
start_mysql() {
    echo "===== 检测并启动MySQL服务 ====="
    if ! systemctl status ${MYSQL_SERVICE} &>/dev/null; then
        if command -v service &>/dev/null; then
            echo "--- 使用service启动MySQL ---"
            service ${MYSQL_SERVICE} start
        elif command -v systemctl &>/dev/null; then
            echo "--- 使用systemctl启动MySQL ---"
            systemctl start ${MYSQL_SERVICE}
        else
            echo "无法检测MySQL启动方式，请手动启动"
            exit 1
        fi
        
        # 等待服务启动
        echo "--- 等待MySQL服务启动 ---"
        for i in {1..10}; do
            if mysqladmin ping -h localhost -u root -p &>/dev/null; then
                echo "MySQL服务启动成功"
                break
            fi
            sleep 1
        done
        
        # 检查最终状态
        if ! mysqladmin ping -h localhost -u root -p &>/dev/null; then
            echo "MySQL服务启动失败，请手动检查"
            exit 1
        fi
    else
        echo "MySQL服务已运行"
    fi
    echo "--------------------------------------------------"
}

# 定义Hive执行命令函数
execute_hive() {
    echo "===== 执行Hive命令: $1 ====="
    hive -e "$1"
    echo "--------------------------------------------------"
}

# 主流程
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

# 3. 查询统计分析
execute_hive "
-- 计算数据集总行数
select count(*) from raw_user;

-- 统计不重复用户数
select count(distinct uid) from raw_user;

-- 统计完全唯一的用户行为记录数量
select count(*) from (
    select uid, item_id, behavior_type, item_category, visit_date, province 
    from raw_user 
    group by uid, item_id, behavior_type, item_category, visit_date, province 
    having count(*)=1
) a;
"

# 4. 计算用户转化漏斗
execute_hive "
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
END;
"

echo "===== 大规模数据集Hive分析完成 ====="
echo "分析内容：数据库查看、基础查询、统计分析、转化漏斗计算"
echo "数据来源：dblab.raw_user（大规模用户行为数据集）"