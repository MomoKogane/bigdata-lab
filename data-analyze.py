#!/usr/bin/env python3
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyecharts import options as opts
from pyecharts.charts import Map
import numpy as np
import traceback
import logging
import os
import sys
from datetime import datetime
import psutil
import time
import argparse
import matplotlib as mpl

# ������־ϵͳ
def setup_logging():
    """������־��¼ϵͳ"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"user_behavior_analysis_{timestamp}.log")
    
    # ��������ɫ�Ŀ���̨��־��ʽ
    class ColorFormatter(logging.Formatter):
        COLORS = {
            logging.INFO: '\033[92m',  # ��ɫ
            logging.WARNING: '\033[93m',  # ��ɫ
            logging.ERROR: '\033[91m',  # ��ɫ
            logging.CRITICAL: '\033[91m\033[1m'  # ��ɫ�Ӵ�
        }
        RESET = '\033[0m'
        
        def format(self, record):
            color = self.COLORS.get(record.levelno, '')
            message = super().format(record)
            return f"{color}{message}{self.RESET}" if color else message
    
    logger = logging.getLogger('user_behavior_analysis')
    logger.setLevel(logging.INFO)
    
    # �ļ�������
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # ����̨������
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ȷ������������ʾ (Ubuntu 18.04����)
try:
    # ����ѡ����������
    chinese_fonts = [
        "SimHei", 
        "Microsoft YaHei",
        "WenQuanYi Micro Hei",
        "WenQuanYi Zen Hei",
        "Noto Sans CJK SC",
        "Source Han Sans SC",
        "Droid Sans Fallback",
        "sans-serif"
    ]
    
    # ��ȡϵͳ��������
    available_fonts = [f.name.lower() for f in mpl.font_manager.fontManager.ttflist]
    logger.info(f"������������: {len(available_fonts)}")
    
    # �ҵ���һ�����õ���������
    selected_font = None
    for font in chinese_fonts:
        # ����������ƻ����
        if font.lower() in available_fonts:
            selected_font = font
            break
        # ��������ļ��Ƿ���ڣ����ɿ��ķ�����
        try:
            font_files = mpl.font_manager.findfont(font, fallback_to_default=False)
            if font_files and font_files != mpl.font_manager.get_default_font():
                selected_font = font
                break
        except ValueError:
            continue  # ������岻���ڣ����������һ��

    if selected_font:
        # ����ȫ�����壨��Matplotlib��Ч��
        plt.rcParams['font.family'] = selected_font
        # ����Seaborn���壨��Ҫ�������ã�
        sns.set(font=selected_font)
        # ����pyechartsȫ��ѡ��
        from pyecharts.globals import CurrentConfig
        CurrentConfig.GLOBAL_FONT = selected_font
        
        logger.info(f"�ɹ�������������: {selected_font}")
    else:
        logger.warning("δ�ҵ����ʵ��������壬����ʹ��Ĭ������")
        # ǿ�����û��˷���
        plt.rcParams['font.family'] = ['sans-serif']
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'sans-serif']
    
    plt.rcParams['axes.unicode_minus'] = False
    logger.info("������ʾ�������")
    
    # ������建�棨��Ҫ����
    mpl.font_manager._rebuild()
except Exception as e:
    logger.error(f"�������ô���: {str(e)}")
    logger.error(traceback.format_exc())


### 1. ����MySQL���ݿⲢ��ȡ���� (֧�ַֿ��ȡ)
def get_data_from_mysql(use_aggregated_query=False, chunk_size=100000):
    """��MySQL��ȡ�û���Ϊ���ݣ�֧�ַֿ��ȡ��Ԥ�ۺ�"""
    logger.info("�����������ݿⲢ��ȡ����...")
    
    # ��¼�ڴ�ʹ�����
    start_mem = psutil.virtual_memory().used / (1024 ** 2)  # MB
    
    conn = None
    max_retries = 3
    retry_delay = 5  # ��
    
    try:
        for attempt in range(max_retries):
            try:
                conn = pymysql.connect(
                    host='127.0.0.1',
                    port=3306,
                    user='root',
                    password='root',
                    database='dblab',
                    connect_timeout=30,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
                logger.info("���ݿ����ӳɹ�")
                break
            except pymysql.OperationalError as oe:
                if attempt < max_retries - 1:
                    logger.warning(f"���ݿ�����ʧ��({str(oe)})��{retry_delay}�������... (���� {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"���ݿ�����ʧ��: {str(oe)}")
        
        # �����Ƿ����
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'user_action'")
            if not cursor.fetchone():
                logger.error("����: ���ݿ���û����Ϊ 'user_action' �ı�")
                return None
        
        # �������ݼ���Сѡ���ѯ��ʽ
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM user_action")
            total_rows = cursor.fetchone()['COUNT(*)']
            logger.info(f"���ݱ�������: {total_rows:,}")
            
            # �����ݼ�ʹ��Ԥ�ۺϲ�ѯ
            if total_rows > 5000000 or use_aggregated_query:  # 500��������ʹ�þۺ�
                logger.info("��⵽�����ݼ���ʹ��Ԥ�ۺϲ�ѯ...")
                query = """
                SELECT 
                    behavior_type,
                    DATE_FORMAT(visit_date, '%%Y-%%m') AS month,
                    DATE_FORMAT(visit_date, '%%d') AS day,
                    item_category,
                    province,
                    COUNT(DISTINCT uid) AS user_count,
                    COUNT(*) AS total_actions
                FROM user_action
                GROUP BY behavior_type, month, day, item_category, province
                """
                logger.info("ִ�оۺϲ�ѯ...")
                data = pd.read_sql(query, conn)
                data['behavior_type_num'] = pd.to_numeric(data['behavior_type'], errors='coerce')
            else:
                logger.info("ʹ���������ݲ�ѯ...")
                query = "SELECT * FROM user_action"
                
                # �����ݼ��ֿ��ȡ
                if total_rows > 1000000:
                    logger.info(f"���ݼ��ϴ�({total_rows:,}��)�����÷ֿ��ȡ(chunk_size={chunk_size})")
                    chunks = []
                    for i, chunk in enumerate(pd.read_sql(query, conn, chunksize=chunk_size)):
                        chunks.append(chunk)
                        logger.info(f"�Ѷ�ȡ���� #{i+1}, �ۼ�����: {len(pd.concat(chunks, ignore_index=True)):,}")
                    data = pd.concat(chunks, ignore_index=True)
                else:
                    data = pd.read_sql(query, conn)
        
        # ��������Ƿ�Ϊ��
        if data.empty:
            logger.warning("����: ���ݿ��ѯ���ؿս��")
            return None
        
        # �ڴ�ʹ�ñ���
        end_mem = psutil.virtual_memory().used / (1024 ** 2)
        logger.info(f"�ɹ���ȡ���ݣ�����: {len(data):,}, �ڴ�ʹ��: {end_mem - start_mem:.2f} MB")
        return data
    
    except pymysql.OperationalError as oe:
        logger.error(f"���ݿ����Ӵ���: {str(oe)}")
        logger.error("����: 1. MySQL�����Ƿ����� 2. ���ݿ������Ƿ���ȷ")
        return None
    
    except Exception as e:
        logger.error(f"���ݿ��������: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    
    finally:
        if conn:
            conn.close()
            logger.info("���ݿ������ѹر�")


### 2. ����Ԥ������ (�Ż��ڴ�ʹ��)
def preprocess_data(data):
    """ת���������͡���ȡ�·ݲ������ֵ"""
    logger.info("���ڽ�������Ԥ����...")
    start_time = time.time()
    
    try:
        # ���������Ч��
        if data is None or data.empty:
            logger.error("����: ����Ϊ�գ��޷�����Ԥ����")
            return None
        
        # ��¼ԭʼ������״
        original_shape = data.shape
        
        # ��鲢�����ֵ
        null_counts = data.isnull().sum()
        if null_counts.sum() > 0:
            null_columns = null_counts[null_counts > 0]
            logger.warning(f"���ݴ��ڿ�ֵ�����п�ֵ����:\n{null_columns}")
            
            # �����ֵ����
            null_percent = (data.isnull().mean() * 100).round(2)
            high_null_cols = null_percent[null_percent > 30]
            
            if not high_null_cols.empty:
                logger.warning(f"����: �����п�ֵ��������30%:\n{high_null_cols}")
                # ɾ����ֵ�������ߵ���
                data = data.drop(columns=high_null_cols.index.tolist())
                logger.info(f"��ɾ����ȱʧֵ��: {high_null_cols.index.tolist()}")
            
            # ɾ��ʣ���ֵ����
            data = data.dropna()
            logger.info(f"���Ƴ���ֵ���Ƴ�����: {original_shape[0] - data.shape[0]}")
        
        # ���ؼ����Ƿ����
        required_columns = ['behavior_type', 'visit_date', 'uid', 'item_category', 'province']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.error(f"����: ����ȱ�ٹؼ���: {missing_columns}")
            return None
        
        # ת����Ϊ����Ϊ��ֵ��
        if 'behavior_type_num' not in data.columns:  # ����δԤ�ۺ�ʱִ��
            try:
                data['behavior_type_num'] = pd.to_numeric(data['behavior_type'], errors='coerce')
                if data['behavior_type_num'].isna().any():
                    invalid_count = data['behavior_type_num'].isna().sum()
                    logger.warning(f"����: ���� {invalid_count} ����Ч��Ϊ���ͼ�¼")
                    data = data.dropna(subset=['behavior_type_num'])
                data['behavior_type_num'] = data['behavior_type_num'].astype(int)
            except Exception as e:
                logger.error(f"��Ϊ����ת������: {str(e)}")
                return None
        
        # ����������ȡ�·�
        if 'month' not in data.columns:  # ����δԤ�ۺ�ʱִ��
            try:
                data['month'] = data['visit_date'].astype(str).str[5:7]
                # ��֤�·ݸ�ʽ
                invalid_months = data[~data['month'].str.match(r'^\d{2}$')]
                if not invalid_months.empty:
                    logger.warning(f"����: ���� {len(invalid_months)} ����Ч���ڼ�¼")
                    data = data.drop(invalid_months.index)
            except Exception as e:
                logger.error(f"���ڴ������: {str(e)}")
                return None
        
        # ��ȡ�����е���
        if 'day' not in data.columns:  # ����δԤ�ۺ�ʱִ��
            try:
                data['day'] = data['visit_date'].astype(str).str[8:10]
            except:
                logger.warning("������ȡ�����ֶ�ʧ�ܣ����ܲ�Ӱ���������")
        
        # ����ʱ�䱨��
        elapsed = time.time() - start_time
        logger.info(f"����Ԥ������ɣ�����: {len(data):,}, ��ʱ: {elapsed:.2f}��")
        return data
    
    except Exception as e:
        logger.error(f"����Ԥ��������з�������: {str(e)}")
        logger.error(traceback.format_exc())
        return None


### 3. ��������Ϊ�ֲ����ӻ���ֱ��ͼ��
def plot_behavior_distribution(data):
    """ʹ��matplotlib������Ϊ���ͷֲ�ֱ��ͼ"""
    try:
        if data is None or data.empty:
            logger.error("����: ����Ч�������ڻ�����Ϊ�ֲ�ͼ")
            return
        
        logger.info("���ڻ�����������Ϊ���ͷֲ�ֱ��ͼ...")
        
        # �����Ϊ���������Ƿ���Ч
        if 'behavior_type_num' not in data.columns:
            logger.error("����: ������ȱ����Ϊ������")
            return
        
        plt.figure(figsize=(10, 6))
        sns.histplot(data['behavior_type_num'], kde=False, bins=4, color='lightblue')
        plt.title('Consumer Behavior Type Distribution')
        plt.xlabel('Behavior Type (1=Browse, 4=Purchase)')
        plt.ylabel('Frequency')
        plt.xticks([1, 2, 3, 4])
        plt.grid(True, alpha=0.3)
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'behavior_distribution.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"��������Ϊ���ͷֲ�ֱ��ͼ������ɣ��ѱ�����: {output_path}")
    
    except ValueError as ve:
        logger.error(f"��ͼ���ݴ���: {str(ve)}")
    
    except RuntimeError as re:
        logger.error(f"��ͼ����ʱ����: {str(re)}")
    
    except Exception as e:
        logger.error(f"������Ϊ�ֲ�ͼʱ����δ֪����: {str(e)}")
        logger.error(traceback.format_exc())


### 4. ������ǰʮ����Ʒ���ࣨ��״ͼ��
def plot_top_purchased_categories(data):
    """ʹ��seaborn���ƹ�����ǰʮ����Ʒ����"""
    try:
        if data is None or data.empty:
            logger.error("����: ����Ч�������ڻ�����Ʒ����ͼ")
            return
        
        logger.info("���ڷ��������ƹ�����ǰʮ����Ʒ����...")
        
        # ����Ҫ���Ƿ����
        required_cols = ['behavior_type_num', 'item_category']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"����: ������ȱ�ٹؼ���: {missing_cols}")
            return
        
        # ɸѡ������Ϊ
        if 4 not in data['behavior_type_num'].values:
            logger.warning("����: ������û�й�����Ϊ��¼")
            return
        
        buy_data = data[data['behavior_type_num'] == 4].copy()
        
        # ����Ƿ����㹻�Ĺ����¼
        if buy_data.empty:
            logger.warning("����: û�й����¼�����ڷ���")
            return
        
        # ͳ����Ʒ���๺�����
        category_count = buy_data['item_category'].value_counts()
        
        # ����Ƿ����㹻�����ݵ�
        if len(category_count) < 5:
            logger.warning(f"����: ֻ�� {len(category_count)} ����Ʒ���࣬���ڽ����10��")
        
        top_count = min(10, len(category_count))
        category_count = category_count.nlargest(top_count)
        
        plt.figure(figsize=(12, 7))
        ax = sns.barplot(x=category_count.index, y=category_count.values, color='green')
        plt.title('Top 10 Purchased Categories')
        plt.xlabel('product category')
        plt.ylabel('purchase count')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)
        
        # Ϊÿ�����������ֵ��ǩ
        for i, v in enumerate(category_count.values):
            ax.text(i, v + 0.05 * max(category_count.values), f'{v}', ha='center', fontsize=9)
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'top_categories.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"������ǰʮ����Ʒ������״ͼ������ɣ��ѱ�����: {output_path}")
    
    except Exception as e:
        logger.error(f"������Ʒ����ͼʱ��������: {str(e)}")
        logger.error(traceback.format_exc())


### 5. ���·���������Ϊ������ֱ��ͼ��- �޸���
def plot_monthly_behavior(data):
    """ʹ��seaborn����ֱ��ͼ����������Ϊ�ֲ����޸��棩"""
    try:
        if data is None or data.empty:
            logger.error("����: ����Ч�������ڻ����¶���Ϊ�ֲ�ͼ")
            return
        
        logger.info("���ڻ��Ƹ��·���������Ϊ�ֲ�����ֱ��ͼ...")
        
        # ����Ҫ���Ƿ����
        required_cols = ['behavior_type_num', 'month']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"����: ������ȱ�ٹؼ���: {missing_cols}")
            return
        
        # ����·������Ƿ���Ч
        valid_months = data['month'].unique()
        if len(valid_months) < 2:
            logger.warning(f"����: ֻ�� {len(valid_months)} ���·ݵ����ݣ����ܲ��ʺϷ������")
        
        # ʹ��displot����histplot��֧�ַ��������
        g = sns.displot(
            data=data,
            x='behavior_type_num',
            col='month',
            bins=4,
            kde=False,
            color='lightgreen',
            height=5,  # ÿ����ͼ�ĸ߶�
            aspect=1.2,  # ��ͼ��߱�
            col_wrap=min(3, len(valid_months)))  # ÿ�����3����ͼ
        
        # �����ܱ�����������ǩ
        g.fig.suptitle('Monthly Consumer Behavior Distribution', y=1.05)
        g.set_axis_labels("Behavior Type (1=Browse, 4=Purchase)", "Frequency")
        
        # ������ͼ����
        plt.tight_layout()
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'monthly_behavior.png')
        g.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"���·���������Ϊ�ֲ�����ֱ��ͼ������ɣ��ѱ�����: {output_path}")
    
    except Exception as e:
        logger.error(f"�����¶���Ϊ�ֲ�ͼʱ��������: {str(e)}")
        logger.error(traceback.format_exc())


### 6. ��ʡ�ݹ���������������ͼ���ӻ���
def plot_province_purchase(data):
    """ʹ��pyecharts���Ƹ�ʡ�ݹ�������ͼ"""
    try:
        # ע���ͼ��Դ������հ׵�ͼ���⣩
        try:
            from pyecharts.datasets import register_url
            # ����ʹ�����ߵ�ͼ
            register_url("https://echarts-maps.github.io/echarts-china-counties-js/")
            logger.info("�ɹ��������ߵ�ͼ��Դ")
        except Exception as e:
            logger.warning(f"�޷��������ߵ�ͼ��Դ: {str(e)}��ʹ�����õ�ͼ")
        
        if data is None or data.empty:
            logger.error("����: ����Ч�������ڻ���ʡ�ݹ����ͼ")
            return
        
        logger.info("���ڷ�����ʡ�ݹ����������Ƶ�ͼ...")
        
        # ����Ҫ���Ƿ����
        required_cols = ['behavior_type_num', 'province']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"����: ������ȱ�ٹؼ���: {missing_cols}")
            return
        
        # ɸѡ������Ϊ
        if 4 not in data['behavior_type_num'].values:
            logger.warning("����: ������û�й�����Ϊ��¼")
            return
        
        buy_data = data[data['behavior_type_num'] == 4].copy()
        
        # ����Ƿ����㹻�Ĺ����¼
        if buy_data.empty:
            logger.warning("����: û�й����¼�����ڷ���")
            return
        
        # ͳ��ʡ�ݹ�����
        province_count = buy_data['province'].value_counts().reset_index()
        province_count.columns = ['province', 'count']
        
        # ���ʡ�������Ƿ���Ч
        if province_count['province'].isna().any():
            logger.warning("����: ���ڿ�ʡ�ݼ�¼���ѹ���")
            province_count = province_count.dropna(subset=['province'])
        
        # ��׼��ʡ������
        province_mapping = {
            '����': '������', '�Ϻ�': '�Ϻ���', '���': '�����', '����': '������',
            '�ӱ�': '�ӱ�ʡ', 'ɽ��': 'ɽ��ʡ', '����': '����ʡ', '����': '����ʡ',
            '������': '������ʡ', '����': '����ʡ', '�㽭': '�㽭ʡ', '����': '����ʡ',
            '����': '����ʡ', '����': '����ʡ', 'ɽ��': 'ɽ��ʡ', '����': '����ʡ',
            '����': '����ʡ', '����': '����ʡ', '�㶫': '�㶫ʡ', '����': '����ʡ',
            '�Ĵ�': '�Ĵ�ʡ', '����': '����ʡ', '����': '����ʡ', '����': '����ʡ',
            '����': '����ʡ', '�ຣ': '�ຣʡ', '̨��': '̨��ʡ',
            '���ɹ�': '���ɹ�������', '����': '����׳��������', '����': '����������',
            '����': '���Ļ���������', '�½�': '�½�ά���������',
            '���': '����ر�������', '����': '�����ر�������'
        }
        
        province_count['province'] = province_count['province'].map(province_mapping).fillna(province_count['province'])
        
        # ת��Ϊpyecharts��Ҫ�����ݸ�ʽ
        map_data = [[prov, count] for prov, count in zip(province_count['province'], province_count['count'])]
        
        # ������ͼ
        min_value = province_count['count'].min()
        max_value = province_count['count'].max()
        
        # ��������ֵΪ0���������
        if min_value == max_value == 0:
            min_value, max_value = 0, 1  # ������������
        
        china_map = (
            Map()
            .add("������", map_data, "china", is_map_symbol_show=False)
            .set_global_opts(
                title_opts=opts.TitleOpts(title="��ʡ�ݹ������ֲ�"),
                visualmap_opts=opts.VisualMapOpts(
                    min_=min_value,
                    max_=max_value,
                    range_text=["Low", "High"],
                    range_color=["lightblue", "red"],
                    orient="vertical",
                    pos_right="10%",
                    pos_top="center"
                )
            )
        )
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", "province_purchase_map.html")
        china_map.render(output_path)
        logger.info(f"��ʡ�ݹ�������ͼ������ɣ��ѱ�����: {output_path}")
    
    except Exception as e:
        logger.error(f"����ʡ�ݹ����ͼʱ��������: {str(e)}")
        logger.error(traceback.format_exc())


### 7. ÿ���û���Ϊ���Ʒ���������ͼ��
def plot_daily_behavior_trend(data):
    """ʹ��matplotlib����ÿ�ո�����Ϊ����"""
    try:
        if data is None or data.empty:
            logger.error("����: ����Ч�������ڻ���ÿ����Ϊ����ͼ")
            return
        
        logger.info("���ڷ���ÿ���û���Ϊ����...")
        
        # ����Ҫ���Ƿ����
        required_cols = ['day', 'behavior_type_num', 'uid']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"����: ������ȱ�ٹؼ���: {missing_cols}")
            return
        
        # �����ں���Ϊ���ͷ���ͳ��
        daily_trend = data.groupby(['day', 'behavior_type_num'])['uid'].nunique().reset_index()
        
        # ����Ƿ����㹻�����ݵ�
        if len(daily_trend) < 5:
            logger.warning(f"����: ֻ�� {len(daily_trend)} �����ݵ㣬���ܲ��ʺ����Ʒ���")
        
        # ת�����ݸ�ʽ
        daily_trend_pivot = daily_trend.pivot(index='day', columns='behavior_type_num', values='uid').fillna(0)
        
        # ����Ƿ�����Ϊ��������
        if daily_trend_pivot.empty:
            logger.warning("����: û���㹻�����ݴ�������ͼ")
            return
        
        plt.figure(figsize=(14, 7))
        
        # ��ȡ������Ϊ����
        behavior_types = sorted(daily_trend['behavior_type_num'].unique())
        
        # Ϊÿ����Ϊ���ͻ�������
        for behavior in behavior_types:
            if behavior in daily_trend_pivot.columns:
                plt.plot(daily_trend_pivot.index, daily_trend_pivot[behavior], 
                         marker='o', label=f'Behavior {behavior}')
        
        plt.title('Daily User Behavior Trend')
        plt.xlabel('Date')
        plt.ylabel('User Count (Distinct Uid)')
        plt.legend(title='Behavior Type')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha='right')
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'daily_behavior_trend.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"ÿ����Ϊ���Ʒ�����ɣ��ѱ�����: {output_path}")
    
    except Exception as e:
        logger.error(f"����ÿ����Ϊ����ͼʱ��������: {str(e)}")
        logger.error(traceback.format_exc())


### 8. ��Ʒ��������Ϊ���͹�������������ͼ��
def plot_category_behavior_correlation(data):
    """ʹ��seaborn������Ʒ��������Ϊ���͹�������ͼ"""
    try:
        if data is None or data.empty:
            logger.error("����: ����Ч�������ڻ�����Ʒ�����������ͼ")
            return
        
        logger.info("���ڷ�����Ʒ��������Ϊ���͹���...")
        
        # ����Ҫ���Ƿ����
        required_cols = ['item_category', 'behavior_type_num']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"����: ������ȱ�ٹؼ���: {missing_cols}")
            return
        
        # ͳ��ÿ����Ʒ�����¸���Ϊ���͵�����
        category_behavior = data.groupby(['item_category', 'behavior_type_num'])['uid'].count().reset_index()
        
        # ����Ƿ����㹻�����ݵ�
        if len(category_behavior) < 10:
            logger.warning(f"����: ֻ�� {len(category_behavior)} �����ݵ㣬���ܲ��ʺ�����ͼ����")
        
        # ת��Ϊ�����
        category_behavior_pivot = category_behavior.pivot(
            index='item_category', 
            columns='behavior_type_num', 
            values='uid'
        ).fillna(0)
        
        # ����Ƿ����㹻������
        if category_behavior_pivot.empty or category_behavior_pivot.shape[0] < 5:
            logger.warning("����: û���㹻�����ݴ�������ͼ")
            return
        
        # ѡ����Ϊ��������ǰ20����Ʒ����
        top_categories = category_behavior_pivot.sum(axis=1).nlargest(20).index
        category_behavior_pivot = category_behavior_pivot.loc[top_categories]
        
        plt.figure(figsize=(15, 10))
        sns.heatmap(category_behavior_pivot, annot=True, fmt='g', cmap='YlGnBu', 
                   cbar_kws={'label': '��Ϊ����'}, annot_kws={'size': 8})
        plt.title('Category and Behavior Type Correlation Heatmap')
        plt.xlabel('behavior type (1=Browse, 4=Purchase)')
        plt.ylabel('category id')
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'category_behavior_heatmap.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"��Ʒ��������Ϊ����������ɣ��ѱ�����: {output_path}")
    
    except Exception as e:
        logger.error(f"������Ʒ�����������ͼʱ��������: {str(e)}")
        logger.error(traceback.format_exc())


### 9. �û��������
def plot_user_retention(data):
    """�����û�������������ӻ�"""
    try:
        if data is None or data.empty:
            logger.error("����: ����Ч���������û��������")
            return
        
        logger.info("���ڽ����û��������...")
        
        # ����Ҫ���Ƿ����
        required_cols = ['uid', 'visit_date']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"����: ������ȱ�ٹؼ���: {missing_cols}")
            return
        
        # �������ݸ����Ա����޸�ԭʼ����
        retention_data = data[['uid', 'visit_date']].copy()
        
        # ת�����ڸ�ʽ
        try:
            retention_data['visit_date'] = pd.to_datetime(retention_data['visit_date'])
        except Exception as e:
            logger.error(f"����ת������: {str(e)}")
            return
        
        # �����û��״η�������
        first_visit = retention_data.groupby('uid')['visit_date'].min().reset_index()
        first_visit.columns = ['uid', 'first_visit']
        
        # �ϲ��״η�������
        retention_data = pd.merge(retention_data, first_visit, on='uid')
        
        # ����ʱ���
        retention_data['date_diff'] = (retention_data['visit_date'] - retention_data['first_visit']).dt.days
        
        # ɸѡ30��������
        retention_data = retention_data[retention_data['date_diff'] <= 30]
        
        # ����Ƿ����㹻�û�
        unique_users = retention_data['uid'].nunique()
        if unique_users < 100:
            logger.warning(f"����: ֻ�� {unique_users} ���û�������������ܲ�׼ȷ")
        
        # ����������
        retention_rates = retention_data.groupby('date_diff')['uid'].nunique() / unique_users * 100
        
        # ����Ƿ����㹻�����ݵ�
        if retention_rates.empty:
            logger.warning("����: û���㹻���ݼ���������")
            return
        
        plt.figure(figsize=(12, 6))
        plt.plot(retention_rates.index, retention_rates.values, marker='o', color='red')
        plt.axhline(y=50, color='gray', linestyle='--', alpha=0.5)
        plt.title('User Retention Rate Over 30 Days')
        plt.xlabel('Days Since First Visit')
        plt.ylabel('Retention Rate (%)')
        plt.grid(True, alpha=0.3)
        plt.ylim(0, 105)  # ȷ���ٷֱ���ʾ����
        
        # ��ǹؼ�������������
        key_days = [0, 1, 3, 7, 14, 30]
        for day in key_days:
            if day in retention_rates.index:
                plt.text(day, retention_rates[day] + 2, f'{retention_rates[day]:.1f}%', ha='center')
        
        # �������Ŀ¼
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'user_retention.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"�û����������ɣ��ѱ�����: {output_path}")
    
    except Exception as e:
        logger.error(f"�û�������������з�������: {str(e)}")
        logger.error(traceback.format_exc())


### 10. �������������������� (��Ӵ����ݼ��Ż�)
def main():
    # ���ȼ��ؼ�����
    logger.info("���ؼ�����...")
    try:
        import pymysql, pandas, seaborn, pyecharts
        logger.info("���б��������Ѱ�װ")
    except ImportError as e:
        missing_module = str(e).split(" ")[-1]
        logger.error(f"ȱ�ٹؼ�����: {missing_module}")
        logger.error("��ִ���������װ��������:")
        logger.error("pip install pymysql pandas seaborn pyecharts psutil")
        return  # �˳�����

    parser = argparse.ArgumentParser(description='�û���Ϊ���ݷ���')
    parser.add_argument('--aggregate', action='store_true', help='ʹ��Ԥ�ۺϲ�ѯ�Ż������ݼ�')
    parser.add_argument('--chunk-size', type=int, default=100000, help='�ֿ��ȡ��С(Ĭ��100,000)')
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info(f"{'�û���Ϊ���ݷ�����������':^70}")
    logger.info(f"{'����: ':<20} Ԥ�ۺ�={args.aggregate}, �ֿ��С={args.chunk_size}")
    logger.info("="*70)
    
    try:
        # 1. ��ȡ����
        logger.info(">>> ����1: �����ݿ��ȡ����")
        data = get_data_from_mysql(
            use_aggregated_query=args.aggregate, 
            chunk_size=args.chunk_size
        )
        if data is None or data.empty:
            logger.error("���ݻ�ȡʧ�ܣ������˳�")
            return
        
        # 2. Ԥ��������
        logger.info(">>> ����2: ����Ԥ����")
        processed_data = preprocess_data(data)
        if processed_data is None or processed_data.empty:
            logger.error("����Ԥ����ʧ�ܣ������˳�")
            return
        
        # 3. ִ�и�����ӻ����� (�������ݹ�ģ����)
        logger.info(">>> ����3: ��ʼ���ݷ�������ӻ�")
        
        # ���������б�
        analysis_tasks = [
            ("��Ϊ���ͷֲ�", plot_behavior_distribution),
            ("��Ʒ�������", plot_top_purchased_categories),
            ("�¶���Ϊ����", plot_monthly_behavior),
            ("ʡ�ݹ������", plot_province_purchase),
            ("ÿ����Ϊ����", plot_daily_behavior_trend),
            ("��Ʒ��Ϊ����", plot_category_behavior_correlation),
            ("�û��������", plot_user_retention)
        ]
        
        # �����ݼ������߳ɱ�����
        if len(processed_data) > 5000000:  # 500��������
            logger.warning("��⵽�����ݼ��������߳ɱ���������")
            skip_tasks = ["��Ʒ��Ϊ����", "�û��������"]
            analysis_tasks = [t for t in analysis_tasks if t[0] not in skip_tasks]
            logger.info(f"��ִ�е�����: {[t[0] for t in analysis_tasks]}")
        
        # ִ����������
        for task_name, task_func in analysis_tasks:
            try:
                logger.info(f"{'='*30} ��ʼ����: {task_name} {'='*30}")
                start_time = time.time()
                
                task_func(processed_data)
                
                elapsed = time.time() - start_time
                logger.info(f"{'='*30} �������: {task_name} [��ʱ: {elapsed:.2f}��] {'='*30}")
            except Exception as e:
                logger.error(f"���� '{task_name}' ִ��ʧ��: {str(e)}")
                logger.error(traceback.format_exc())
                logger.info(f"�������� '{task_name}'������ִ�к�������")
        
        logger.info("="*70)
        logger.info(f"{'�������ݷ����������':^70}")
        logger.info("="*70)
    
    except KeyboardInterrupt:
        logger.warning("�����û��ж�")
    
    except Exception as e:
        logger.error(f"��������δ������쳣: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("�����쳣��ֹ")
    
    finally:
        # �������ǰ��������
        plt.close('all')  # �ر�����matplotlibͼ��
        logger.info("����ִ�н���")


if __name__ == "__main__":
    # ���ϵͳ��Դ���
    logger.info(f"ϵͳ�ڴ�����: {psutil.virtual_memory().total / (1024 ** 3):.2f} GB")
    logger.info(f"CPU������: {psutil.cpu_count()}")
    logger.info(f"Python�汾: {sys.version}")
    
    main()