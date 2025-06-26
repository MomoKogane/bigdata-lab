#!/bin/bash

# �������·��

MYSQL_SERVICE="mysql"

# ����Hadoop����
start_hadoop() {
    echo "===== ��Ⲣ����Hadoop���� ====="
    local namenode_running=$(jps | grep -c NameNode)
    local resourcemanager_running=$(jps | grep -c ResourceManager)
    
    if [ ${namenode_running} -eq 0 ] || [ ${resourcemanager_running} -eq 0 ]; then
        echo "--- ����Hadoop��Ⱥ ---"
        start-all.sh
        
        # �ȴ��������������30�룩
        echo "--- �ȴ�Hadoop�������� ---"
        for i in {1..30}; do
            local nn=$(jps | grep -c NameNode)
            local rm=$(jps | grep -c ResourceManager)
            if [ ${nn} -gt 0 ] && [ ${rm} -gt 0 ]; then
                echo "Hadoop���������ɹ�"
                break
            fi
            sleep 1
        done
        
        # �������״̬
        if [ ${nn} -eq 0 ] || [ ${rm} -eq 0 ]; then
            echo "Hadoop��������ʧ�ܣ����ֶ����"
            exit 1
        fi
    else
        echo "Hadoop����������"
    fi
    echo "--------------------------------------------------"
}

# ����MySQL����
start_mysql() {
    echo "===== ��Ⲣ����MySQL���� ====="
    if ! systemctl status ${MYSQL_SERVICE} &>/dev/null; then
        if command -v service &>/dev/null; then
            echo "--- ʹ��service����MySQL ---"
            service ${MYSQL_SERVICE} start
        elif command -v systemctl &>/dev/null; then
            echo "--- ʹ��systemctl����MySQL ---"
            systemctl start ${MYSQL_SERVICE}
        else
            echo "�޷����MySQL������ʽ�����ֶ�����"
            exit 1
        fi
        
        # �ȴ���������
        echo "--- �ȴ�MySQL�������� ---"
        for i in {1..10}; do
            if mysqladmin ping -h localhost -u root -p &>/dev/null; then
                echo "MySQL���������ɹ�"
                break
            fi
            sleep 1
        done
        
        # �������״̬
        if ! mysqladmin ping -h localhost -u root -p &>/dev/null; then
            echo "MySQL��������ʧ�ܣ����ֶ����"
            exit 1
        fi
    else
        echo "MySQL����������"
    fi
    echo "--------------------------------------------------"
}

# ����Hiveִ�������
execute_hive() {
    echo "===== ִ��Hive����: $1 ====="
    hive -e "$1"
    echo "--------------------------------------------------"
}

# ������
start_hadoop
start_mysql

# 1. �鿴���ݿ�ͱ���Ϣ
execute_hive "
-- �鿴�������ݿ�
show databases;

-- ʹ��dblab���ݿ�
use dblab;

-- �鿴���ݿ��еı�
show tables;

-- �鿴raw_user��ṹ
describe raw_user;

-- �鿴�������
show create table raw_user;
"

# 2. �򵥲�ѯ����
execute_hive "
-- �鿴ǰ10���û�����Ϊ����
select behavior_type from raw_user limit 10;

-- ��ѯǰ20λ�û�������Ʒ��ʱ�����Ʒ���ࣨbehavior_type=4��ʾ����
select visit_date, item_category 
from raw_user 
where behavior_type=4 
limit 20;

-- ����Ƕ������ѯǰ20����¼����Ʒ����Ͳ������
select e.bh, e.it 
from (
    select behavior_type as bh, item_category as it 
    from raw_user
) as e 
limit 20;
"

# 3. ��ѯͳ�Ʒ���
execute_hive "
-- �������ݼ�������
select count(*) from raw_user;

-- ͳ�Ʋ��ظ��û���
select count(distinct uid) from raw_user;

-- ͳ����ȫΨһ���û���Ϊ��¼����
select count(*) from (
    select uid, item_id, behavior_type, item_category, visit_date, province 
    from raw_user 
    group by uid, item_id, behavior_type, item_category, visit_date, province 
    having count(*)=1
) a;
"

# 4. �����û�ת��©��
execute_hive "
WITH stage_users AS (
    SELECT 
        CASE behavior_type
            WHEN 1 THEN '���'
            WHEN 2 THEN '���빺�ﳵ'
            WHEN 3 THEN '�ղ�'
            WHEN 4 THEN '����'
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
            WHEN '���' THEN 1 
            WHEN '���빺�ﳵ' THEN 2 
            WHEN '�ղ�' THEN 3 
            WHEN '����' THEN 4 
        END) AS browse_users
    FROM stage_users
)
SELECT 
    stage,
    users,
    CONCAT(ROUND(users * 100.0 / browse_users, 2), '%') AS conversion_rate
FROM funnel_data
ORDER BY CASE stage 
    WHEN '���' THEN 1 
    WHEN '���빺�ﳵ' THEN 2 
    WHEN '�ղ�' THEN 3 
    WHEN '����' THEN 4 
END;
"

echo "===== ���ģ���ݼ�Hive������� ====="
echo "�������ݣ����ݿ�鿴��������ѯ��ͳ�Ʒ�����ת��©������"
echo "������Դ��dblab.raw_user�����ģ�û���Ϊ���ݼ���"