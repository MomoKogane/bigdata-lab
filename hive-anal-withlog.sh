#!/bin/bash

# �������·������־�ļ�
MYSQL_SERVICE="mysql"
LOG_FILE="hive_analysis_$(date +%Y%m%d_%H%M%S).log"
RESULTS_FILE="analysis_results.txt"

# ��ʼ������ļ�
echo "===== Hive ����������� =====" > "$RESULTS_FILE"
echo "����ʱ��: $(date)" >> "$RESULTS_FILE"
echo "============================" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# ��־��¼����
log() {
    echo "$1" | tee -a "$LOG_FILE"
}

# �����¼����
record_result() {
    echo "$1" | tee -a "$RESULTS_FILE" "$LOG_FILE"
}

# ����Hadoop����
start_hadoop() {
    log "===== ��Ⲣ����Hadoop���� ====="
    local namenode_running=$(jps | grep -c NameNode)
    local resourcemanager_running=$(jps | grep -c ResourceManager)
    
    if [ ${namenode_running} -eq 0 ] || [ ${resourcemanager_running} -eq 0 ]; then
        log "--- ����Hadoop��Ⱥ ---"
        start-all.sh >> "$LOG_FILE" 2>&1
        
        # �ȴ��������������30�룩
        log "--- �ȴ�Hadoop�������� ---"
        for i in {1..30}; do
            local nn=$(jps | grep -c NameNode)
            local rm=$(jps | grep -c ResourceManager)
            if [ ${nn} -gt 0 ] && [ ${rm} -gt 0 ]; then
                log "Hadoop���������ɹ�"
                break
            fi
            sleep 1
        done
        
        # �������״̬
        if [ ${nn} -eq 0 ] || [ ${rm} -eq 0 ]; then
            log "Hadoop��������ʧ�ܣ����ֶ����"
            exit 1
        fi
    else
        log "Hadoop����������"
    fi
    log "--------------------------------------------------"
}

# ����MySQL����
start_mysql() {
    log "===== ��Ⲣ����MySQL���� ====="
    if ! systemctl status ${MYSQL_SERVICE} &>/dev/null; then
        if command -v service &>/dev/null; then
            log "--- ʹ��service����MySQL ---"
            service ${MYSQL_SERVICE} start >> "$LOG_FILE" 2>&1
        elif command -v systemctl &>/dev/null; then
            log "--- ʹ��systemctl����MySQL ---"
            systemctl start ${MYSQL_SERVICE} >> "$LOG_FILE" 2>&1
        else
            log "�޷����MySQL������ʽ�����ֶ�����"
            exit 1
        fi
        
        # �ȴ���������
        log "--- �ȴ�MySQL�������� ---"
        for i in {1..10}; do
            if mysqladmin ping -h localhost -u root -p &>/dev/null; then
                log "MySQL���������ɹ�"
                break
            fi
            sleep 1
        done
        
        # �������״̬
        if ! mysqladmin ping -h localhost -u root -p &>/dev/null; then
            log "MySQL��������ʧ�ܣ����ֶ����"
            exit 1
        fi
    else
        log "MySQL����������"
    fi
    log "--------------------------------------------------"
}

# ����Hiveִ�������
execute_hive() {
    local hive_command="$1"
    log "===== ִ��Hive����: $hive_command ====="
    log "===== ʹ��dblab���ݿ� ======"
    
    # ִ��Hive���������ض�����־�ļ�
    if hive -e "USE dblab; $hive_command" >> "$LOG_FILE" 2>&1; then
        log "--- Hive����ִ�гɹ� ---"
    else
        log "--- Hive����ִ��ʧ�ܣ��˳�Ԥ����ű� ---"
        exit 1
    fi
    log "--------------------------------------------------"
}

# ִ��Hive��ѯ��������ֵ���
execute_and_capture() {
    local description="$1"
    local query="$2"
    local result_var="$3"
    
    log "===== ִ�в�ѯ: $description ====="
    log "��ѯ: $query"
    
    # ִ�в�ѯ��������
    local result
    result=$(hive -S -e "USE dblab; $query" 2>> "$LOG_FILE")
    
    if [ $? -ne 0 ]; then
        log "��ѯִ��ʧ��"
        exit 1
    fi
    
    # ��¼���
    record_result "? $description: $result"
    eval "$result_var=\"$result\""
    log "��ѯ���: $result"
    log "--------------------------------------------------"
}

# ������
{
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

    # 3. ��ѯͳ�Ʒ��� - ������ֵ���
    record_result "===== �ؼ�ָ����� ====="
    
    local total_rows distinct_users unique_records
    execute_and_capture "���ݼ�������" "select count(*) from raw_user;" total_rows
    execute_and_capture "���ظ��û���" "select count(distinct uid) from raw_user;" distinct_users
    execute_and_capture "��ȫΨһ���û���Ϊ��¼����" \
        "select count(*) from (select uid, item_id, behavior_type, item_category, visit_date, province from raw_user group by uid, item_id, behavior_type, item_category, visit_date, province having count(*)=1) a;" \
        unique_records
    
    # 4. �����û�ת��©�� - ������ϸ���
    record_result "===== �û�ת��©�� ====="
    log "===== �����û�ת��©�� ====="
    
    # ִ��©����ѯ��������������浽����ļ�
    hive -e "USE dblab;
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
        END;" | tee -a "$RESULTS_FILE" >> "$LOG_FILE"
    
    if [ $? -ne 0 ]; then
        log "ת��©����ѯʧ��"
        exit 1
    fi
    
    log "ת��©����ѯ���"
    log "--------------------------------------------------"

    # ���ܱ���
    record_result ""
    record_result "===== ��������ժҪ ====="
    record_result "1. ����������: $total_rows"
    record_result "2. �����û���: $distinct_users"
    record_result "3. Ψһ��Ϊ��¼��: $unique_records"
    record_result "4. ת��©��������Ϸ����"
    record_result ""
    record_result "��ϸ��־: $LOG_FILE"
    record_result "����ʱ��: $(date)"

    log "===== ���ģ���ݼ�Hive������� ====="
    log "�������ݣ����ݿ�鿴��������ѯ��ͳ�Ʒ�����ת��©������"
    log "������Դ��dblab.raw_user�����ģ�û���Ϊ���ݼ���"
    log "�ؼ�����ѱ�����: $RESULTS_FILE"
    log "��ϸ��־�ѱ�����: $LOG_FILE"
} | tee -a "$LOG_FILE"  # ȷ�������������¼����־