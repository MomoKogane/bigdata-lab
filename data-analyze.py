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

# 配置日志系统
def setup_logging():
    """配置日志记录系统"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"user_behavior_analysis_{timestamp}.log")
    
    # 创建带颜色的控制台日志格式
    class ColorFormatter(logging.Formatter):
        COLORS = {
            logging.INFO: '\033[92m',  # 绿色
            logging.WARNING: '\033[93m',  # 黄色
            logging.ERROR: '\033[91m',  # 红色
            logging.CRITICAL: '\033[91m\033[1m'  # 红色加粗
        }
        RESET = '\033[0m'
        
        def format(self, record):
            color = self.COLORS.get(record.levelno, '')
            message = super().format(record)
            return f"{color}{message}{self.RESET}" if color else message
    
    logger = logging.getLogger('user_behavior_analysis')
    logger.setLevel(logging.INFO)
    
    # 文件处理器
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# 确保中文正常显示 (Ubuntu 18.04兼容)
try:
    # 优先选择中文字体
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
    
    # 获取系统可用字体
    available_fonts = [f.name.lower() for f in mpl.font_manager.fontManager.ttflist]
    logger.info(f"可用字体数量: {len(available_fonts)}")
    
    # 找到第一个可用的中文字体
    selected_font = None
    for font in chinese_fonts:
        # 检查字体名称或别名
        if font.lower() in available_fonts:
            selected_font = font
            break
        # 检查字体文件是否存在（更可靠的方法）
        try:
            font_files = mpl.font_manager.findfont(font, fallback_to_default=False)
            if font_files and font_files != mpl.font_manager.get_default_font():
                selected_font = font
                break
        except ValueError:
            continue  # 如果字体不存在，继续检查下一个

    if selected_font:
        # 设置全局字体（对Matplotlib有效）
        plt.rcParams['font.family'] = selected_font
        # 设置Seaborn字体（需要单独设置）
        sns.set(font=selected_font)
        # 设置pyecharts全局选项
        from pyecharts.globals import CurrentConfig
        CurrentConfig.GLOBAL_FONT = selected_font
        
        logger.info(f"成功设置中文字体: {selected_font}")
    else:
        logger.warning("未找到合适的中文字体，尝试使用默认字体")
        # 强制设置回退方案
        plt.rcParams['font.family'] = ['sans-serif']
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'sans-serif']
    
    plt.rcParams['axes.unicode_minus'] = False
    logger.info("中文显示配置完成")
    
    # 清除字体缓存（重要！）
    mpl.font_manager._rebuild()
except Exception as e:
    logger.error(f"字体配置错误: {str(e)}")
    logger.error(traceback.format_exc())


### 1. 连接MySQL数据库并获取数据 (支持分块读取)
def get_data_from_mysql(use_aggregated_query=False, chunk_size=100000):
    """从MySQL获取用户行为数据，支持分块读取和预聚合"""
    logger.info("正在连接数据库并获取数据...")
    
    # 记录内存使用情况
    start_mem = psutil.virtual_memory().used / (1024 ** 2)  # MB
    
    conn = None
    max_retries = 3
    retry_delay = 5  # 秒
    
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
                logger.info("数据库连接成功")
                break
            except pymysql.OperationalError as oe:
                if attempt < max_retries - 1:
                    logger.warning(f"数据库连接失败({str(oe)})，{retry_delay}秒后重试... (尝试 {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"数据库连接失败: {str(oe)}")
        
        # 检查表是否存在
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'user_action'")
            if not cursor.fetchone():
                logger.error("错误: 数据库中没有名为 'user_action' 的表")
                return None
        
        # 根据数据集大小选择查询方式
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM user_action")
            total_rows = cursor.fetchone()['COUNT(*)']
            logger.info(f"数据表总行数: {total_rows:,}")
            
            # 大数据集使用预聚合查询
            if total_rows > 5000000 or use_aggregated_query:  # 500万条以上使用聚合
                logger.info("检测到大数据集，使用预聚合查询...")
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
                logger.info("执行聚合查询...")
                data = pd.read_sql(query, conn)
                data['behavior_type_num'] = pd.to_numeric(data['behavior_type'], errors='coerce')
            else:
                logger.info("使用完整数据查询...")
                query = "SELECT * FROM user_action"
                
                # 大数据集分块读取
                if total_rows > 1000000:
                    logger.info(f"数据集较大({total_rows:,}行)，启用分块读取(chunk_size={chunk_size})")
                    chunks = []
                    for i, chunk in enumerate(pd.read_sql(query, conn, chunksize=chunk_size)):
                        chunks.append(chunk)
                        logger.info(f"已读取区块 #{i+1}, 累计行数: {len(pd.concat(chunks, ignore_index=True)):,}")
                    data = pd.concat(chunks, ignore_index=True)
                else:
                    data = pd.read_sql(query, conn)
        
        # 检查数据是否为空
        if data.empty:
            logger.warning("警告: 数据库查询返回空结果")
            return None
        
        # 内存使用报告
        end_mem = psutil.virtual_memory().used / (1024 ** 2)
        logger.info(f"成功获取数据，行数: {len(data):,}, 内存使用: {end_mem - start_mem:.2f} MB")
        return data
    
    except pymysql.OperationalError as oe:
        logger.error(f"数据库连接错误: {str(oe)}")
        logger.error("请检查: 1. MySQL服务是否运行 2. 数据库配置是否正确")
        return None
    
    except Exception as e:
        logger.error(f"数据库操作错误: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")


### 2. 数据预处理函数 (优化内存使用)
def preprocess_data(data):
    """转换数据类型、提取月份并处理空值"""
    logger.info("正在进行数据预处理...")
    start_time = time.time()
    
    try:
        # 检查数据有效性
        if data is None or data.empty:
            logger.error("错误: 数据为空，无法进行预处理")
            return None
        
        # 记录原始数据形状
        original_shape = data.shape
        
        # 检查并处理空值
        null_counts = data.isnull().sum()
        if null_counts.sum() > 0:
            null_columns = null_counts[null_counts > 0]
            logger.warning(f"数据存在空值，各列空值数量:\n{null_columns}")
            
            # 计算空值比例
            null_percent = (data.isnull().mean() * 100).round(2)
            high_null_cols = null_percent[null_percent > 30]
            
            if not high_null_cols.empty:
                logger.warning(f"警告: 以下列空值比例超过30%:\n{high_null_cols}")
                # 删除空值比例过高的列
                data = data.drop(columns=high_null_cols.index.tolist())
                logger.info(f"已删除高缺失值列: {high_null_cols.index.tolist()}")
            
            # 删除剩余空值的行
            data = data.dropna()
            logger.info(f"已移除空值，移除行数: {original_shape[0] - data.shape[0]}")
        
        # 检查关键列是否存在
        required_columns = ['behavior_type', 'visit_date', 'uid', 'item_category', 'province']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.error(f"错误: 数据缺少关键列: {missing_columns}")
            return None
        
        # 转换行为类型为数值型
        if 'behavior_type_num' not in data.columns:  # 仅当未预聚合时执行
            try:
                data['behavior_type_num'] = pd.to_numeric(data['behavior_type'], errors='coerce')
                if data['behavior_type_num'].isna().any():
                    invalid_count = data['behavior_type_num'].isna().sum()
                    logger.warning(f"警告: 发现 {invalid_count} 条无效行为类型记录")
                    data = data.dropna(subset=['behavior_type_num'])
                data['behavior_type_num'] = data['behavior_type_num'].astype(int)
            except Exception as e:
                logger.error(f"行为类型转换错误: {str(e)}")
                return None
        
        # 从日期中提取月份
        if 'month' not in data.columns:  # 仅当未预聚合时执行
            try:
                data['month'] = data['visit_date'].astype(str).str[5:7]
                # 验证月份格式
                invalid_months = data[~data['month'].str.match(r'^\d{2}$')]
                if not invalid_months.empty:
                    logger.warning(f"警告: 发现 {len(invalid_months)} 条无效日期记录")
                    data = data.drop(invalid_months.index)
            except Exception as e:
                logger.error(f"日期处理错误: {str(e)}")
                return None
        
        # 提取日期中的日
        if 'day' not in data.columns:  # 仅当未预聚合时执行
            try:
                data['day'] = data['visit_date'].astype(str).str[8:10]
            except:
                logger.warning("日期提取部分字段失败，可能不影响后续分析")
        
        # 处理时间报告
        elapsed = time.time() - start_time
        logger.info(f"数据预处理完成，行数: {len(data):,}, 耗时: {elapsed:.2f}秒")
        return data
    
    except Exception as e:
        logger.error(f"数据预处理过程中发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        return None


### 3. 消费者行为分布可视化（直方图）
def plot_behavior_distribution(data):
    """使用matplotlib绘制行为类型分布直方图"""
    try:
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于绘制行为分布图")
            return
        
        logger.info("正在绘制消费者行为类型分布直方图...")
        
        # 检查行为类型数据是否有效
        if 'behavior_type_num' not in data.columns:
            logger.error("错误: 数据中缺少行为类型列")
            return
        
        plt.figure(figsize=(10, 6))
        sns.histplot(data['behavior_type_num'], kde=False, bins=4, color='lightblue')
        plt.title('Consumer Behavior Type Distribution')
        plt.xlabel('Behavior Type (1=Browse, 4=Purchase)')
        plt.ylabel('Frequency')
        plt.xticks([1, 2, 3, 4])
        plt.grid(True, alpha=0.3)
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'behavior_distribution.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"消费者行为类型分布直方图绘制完成，已保存至: {output_path}")
    
    except ValueError as ve:
        logger.error(f"绘图数据错误: {str(ve)}")
    
    except RuntimeError as re:
        logger.error(f"绘图运行时错误: {str(re)}")
    
    except Exception as e:
        logger.error(f"绘制行为分布图时发生未知错误: {str(e)}")
        logger.error(traceback.format_exc())


### 4. 购买量前十的商品分类（柱状图）
def plot_top_purchased_categories(data):
    """使用seaborn绘制购买量前十的商品分类"""
    try:
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于绘制商品分类图")
            return
        
        logger.info("正在分析并绘制购买量前十的商品分类...")
        
        # 检查必要列是否存在
        required_cols = ['behavior_type_num', 'item_category']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"错误: 数据中缺少关键列: {missing_cols}")
            return
        
        # 筛选购买行为
        if 4 not in data['behavior_type_num'].values:
            logger.warning("警告: 数据中没有购买行为记录")
            return
        
        buy_data = data[data['behavior_type_num'] == 4].copy()
        
        # 检查是否有足够的购买记录
        if buy_data.empty:
            logger.warning("警告: 没有购买记录可用于分析")
            return
        
        # 统计商品分类购买次数
        category_count = buy_data['item_category'].value_counts()
        
        # 检查是否有足够的数据点
        if len(category_count) < 5:
            logger.warning(f"警告: 只有 {len(category_count)} 个商品分类，少于建议的10个")
        
        top_count = min(10, len(category_count))
        category_count = category_count.nlargest(top_count)
        
        plt.figure(figsize=(12, 7))
        ax = sns.barplot(x=category_count.index, y=category_count.values, color='green')
        plt.title('Top 10 Purchased Categories')
        plt.xlabel('product category')
        plt.ylabel('purchase count')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)
        
        # 为每个柱子添加数值标签
        for i, v in enumerate(category_count.values):
            ax.text(i, v + 0.05 * max(category_count.values), f'{v}', ha='center', fontsize=9)
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'top_categories.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"购买量前十的商品分类柱状图绘制完成，已保存至: {output_path}")
    
    except Exception as e:
        logger.error(f"绘制商品分类图时发生错误: {str(e)}")
        logger.error(traceback.format_exc())


### 5. 按月分析购买行为（分面直方图）- 修复版
def plot_monthly_behavior(data):
    """使用seaborn分面直方图分析各月行为分布（修复版）"""
    try:
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于绘制月度行为分布图")
            return
        
        logger.info("正在绘制各月份消费者行为分布分面直方图...")
        
        # 检查必要列是否存在
        required_cols = ['behavior_type_num', 'month']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"错误: 数据中缺少关键列: {missing_cols}")
            return
        
        # 检查月份数据是否有效
        valid_months = data['month'].unique()
        if len(valid_months) < 2:
            logger.warning(f"警告: 只有 {len(valid_months)} 个月份的数据，可能不适合分面分析")
        
        # 使用displot代替histplot（支持分面参数）
        g = sns.displot(
            data=data,
            x='behavior_type_num',
            col='month',
            bins=4,
            kde=False,
            color='lightgreen',
            height=5,  # 每个子图的高度
            aspect=1.2,  # 子图宽高比
            col_wrap=min(3, len(valid_months)))  # 每行最多3个子图
        
        # 设置总标题和坐标轴标签
        g.fig.suptitle('Monthly Consumer Behavior Distribution', y=1.05)
        g.set_axis_labels("Behavior Type (1=Browse, 4=Purchase)", "Frequency")
        
        # 调整子图布局
        plt.tight_layout()
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'monthly_behavior.png')
        g.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"各月份消费者行为分布分面直方图绘制完成，已保存至: {output_path}")
    
    except Exception as e:
        logger.error(f"绘制月度行为分布图时发生错误: {str(e)}")
        logger.error(traceback.format_exc())


### 6. 各省份购买欲望分析（地图可视化）
def plot_province_purchase(data):
    """使用pyecharts绘制各省份购买量地图"""
    try:
        # 注册地图资源（解决空白地图问题）
        try:
            from pyecharts.datasets import register_url
            # 尝试使用在线地图
            register_url("https://echarts-maps.github.io/echarts-china-counties-js/")
            logger.info("成功加载在线地图资源")
        except Exception as e:
            logger.warning(f"无法加载在线地图资源: {str(e)}，使用内置地图")
        
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于绘制省份购买地图")
            return
        
        logger.info("正在分析各省份购买量并绘制地图...")
        
        # 检查必要列是否存在
        required_cols = ['behavior_type_num', 'province']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"错误: 数据中缺少关键列: {missing_cols}")
            return
        
        # 筛选购买行为
        if 4 not in data['behavior_type_num'].values:
            logger.warning("警告: 数据中没有购买行为记录")
            return
        
        buy_data = data[data['behavior_type_num'] == 4].copy()
        
        # 检查是否有足够的购买记录
        if buy_data.empty:
            logger.warning("警告: 没有购买记录可用于分析")
            return
        
        # 统计省份购买量
        province_count = buy_data['province'].value_counts().reset_index()
        province_count.columns = ['province', 'count']
        
        # 检查省份数据是否有效
        if province_count['province'].isna().any():
            logger.warning("警告: 存在空省份记录，已过滤")
            province_count = province_count.dropna(subset=['province'])
        
        # 标准化省份名称
        province_mapping = {
            '北京': '北京市', '上海': '上海市', '天津': '天津市', '重庆': '重庆市',
            '河北': '河北省', '山西': '山西省', '辽宁': '辽宁省', '吉林': '吉林省',
            '黑龙江': '黑龙江省', '江苏': '江苏省', '浙江': '浙江省', '安徽': '安徽省',
            '福建': '福建省', '江西': '江西省', '山东': '山东省', '河南': '河南省',
            '湖北': '湖北省', '湖南': '湖南省', '广东': '广东省', '海南': '海南省',
            '四川': '四川省', '贵州': '贵州省', '云南': '云南省', '陕西': '陕西省',
            '甘肃': '甘肃省', '青海': '青海省', '台湾': '台湾省',
            '内蒙古': '内蒙古自治区', '广西': '广西壮族自治区', '西藏': '西藏自治区',
            '宁夏': '宁夏回族自治区', '新疆': '新疆维吾尔自治区',
            '香港': '香港特别行政区', '澳门': '澳门特别行政区'
        }
        
        province_count['province'] = province_count['province'].map(province_mapping).fillna(province_count['province'])
        
        # 转换为pyecharts需要的数据格式
        map_data = [[prov, count] for prov, count in zip(province_count['province'], province_count['count'])]
        
        # 创建地图
        min_value = province_count['count'].min()
        max_value = province_count['count'].max()
        
        # 处理所有值为0的特殊情况
        if min_value == max_value == 0:
            min_value, max_value = 0, 1  # 避免除以零错误
        
        china_map = (
            Map()
            .add("购买量", map_data, "china", is_map_symbol_show=False)
            .set_global_opts(
                title_opts=opts.TitleOpts(title="各省份购买量分布"),
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
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", "province_purchase_map.html")
        china_map.render(output_path)
        logger.info(f"各省份购买量地图绘制完成，已保存至: {output_path}")
    
    except Exception as e:
        logger.error(f"绘制省份购买地图时发生错误: {str(e)}")
        logger.error(traceback.format_exc())


### 7. 每日用户行为趋势分析（折线图）
def plot_daily_behavior_trend(data):
    """使用matplotlib绘制每日各类行为趋势"""
    try:
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于绘制每日行为趋势图")
            return
        
        logger.info("正在分析每日用户行为趋势...")
        
        # 检查必要列是否存在
        required_cols = ['day', 'behavior_type_num', 'uid']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"错误: 数据中缺少关键列: {missing_cols}")
            return
        
        # 按日期和行为类型分组统计
        daily_trend = data.groupby(['day', 'behavior_type_num'])['uid'].nunique().reset_index()
        
        # 检查是否有足够的数据点
        if len(daily_trend) < 5:
            logger.warning(f"警告: 只有 {len(daily_trend)} 个数据点，可能不适合趋势分析")
        
        # 转换数据格式
        daily_trend_pivot = daily_trend.pivot(index='day', columns='behavior_type_num', values='uid').fillna(0)
        
        # 检查是否有行为类型数据
        if daily_trend_pivot.empty:
            logger.warning("警告: 没有足够的数据创建趋势图")
            return
        
        plt.figure(figsize=(14, 7))
        
        # 获取所有行为类型
        behavior_types = sorted(daily_trend['behavior_type_num'].unique())
        
        # 为每种行为类型绘制折线
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
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'daily_behavior_trend.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"每日行为趋势分析完成，已保存至: {output_path}")
    
    except Exception as e:
        logger.error(f"绘制每日行为趋势图时发生错误: {str(e)}")
        logger.error(traceback.format_exc())


### 8. 商品分类与行为类型关联分析（热力图）
def plot_category_behavior_correlation(data):
    """使用seaborn绘制商品分类与行为类型关联热力图"""
    try:
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于绘制商品分类关联热力图")
            return
        
        logger.info("正在分析商品分类与行为类型关联...")
        
        # 检查必要列是否存在
        required_cols = ['item_category', 'behavior_type_num']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"错误: 数据中缺少关键列: {missing_cols}")
            return
        
        # 统计每个商品分类下各行为类型的数量
        category_behavior = data.groupby(['item_category', 'behavior_type_num'])['uid'].count().reset_index()
        
        # 检查是否有足够的数据点
        if len(category_behavior) < 10:
            logger.warning(f"警告: 只有 {len(category_behavior)} 个数据点，可能不适合热力图分析")
        
        # 转换为交叉表
        category_behavior_pivot = category_behavior.pivot(
            index='item_category', 
            columns='behavior_type_num', 
            values='uid'
        ).fillna(0)
        
        # 检查是否有足够的数据
        if category_behavior_pivot.empty or category_behavior_pivot.shape[0] < 5:
            logger.warning("警告: 没有足够的数据创建热力图")
            return
        
        # 选择行为数量最多的前20个商品分类
        top_categories = category_behavior_pivot.sum(axis=1).nlargest(20).index
        category_behavior_pivot = category_behavior_pivot.loc[top_categories]
        
        plt.figure(figsize=(15, 10))
        sns.heatmap(category_behavior_pivot, annot=True, fmt='g', cmap='YlGnBu', 
                   cbar_kws={'label': '行为次数'}, annot_kws={'size': 8})
        plt.title('Category and Behavior Type Correlation Heatmap')
        plt.xlabel('behavior type (1=Browse, 4=Purchase)')
        plt.ylabel('category id')
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'category_behavior_heatmap.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"商品分类与行为关联分析完成，已保存至: {output_path}")
    
    except Exception as e:
        logger.error(f"绘制商品分类关联热力图时发生错误: {str(e)}")
        logger.error(traceback.format_exc())


### 9. 用户留存分析
def plot_user_retention(data):
    """分析用户留存情况并可视化"""
    try:
        if data is None or data.empty:
            logger.error("错误: 无有效数据用于用户留存分析")
            return
        
        logger.info("正在进行用户留存分析...")
        
        # 检查必要列是否存在
        required_cols = ['uid', 'visit_date']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"错误: 数据中缺少关键列: {missing_cols}")
            return
        
        # 创建数据副本以避免修改原始数据
        retention_data = data[['uid', 'visit_date']].copy()
        
        # 转换日期格式
        try:
            retention_data['visit_date'] = pd.to_datetime(retention_data['visit_date'])
        except Exception as e:
            logger.error(f"日期转换错误: {str(e)}")
            return
        
        # 计算用户首次访问日期
        first_visit = retention_data.groupby('uid')['visit_date'].min().reset_index()
        first_visit.columns = ['uid', 'first_visit']
        
        # 合并首次访问日期
        retention_data = pd.merge(retention_data, first_visit, on='uid')
        
        # 计算时间差
        retention_data['date_diff'] = (retention_data['visit_date'] - retention_data['first_visit']).dt.days
        
        # 筛选30天内数据
        retention_data = retention_data[retention_data['date_diff'] <= 30]
        
        # 检查是否有足够用户
        unique_users = retention_data['uid'].nunique()
        if unique_users < 100:
            logger.warning(f"警告: 只有 {unique_users} 个用户，留存分析可能不准确")
        
        # 计算留存率
        retention_rates = retention_data.groupby('date_diff')['uid'].nunique() / unique_users * 100
        
        # 检查是否有足够的数据点
        if retention_rates.empty:
            logger.warning("警告: 没有足够数据计算留存率")
            return
        
        plt.figure(figsize=(12, 6))
        plt.plot(retention_rates.index, retention_rates.values, marker='o', color='red')
        plt.axhline(y=50, color='gray', linestyle='--', alpha=0.5)
        plt.title('User Retention Rate Over 30 Days')
        plt.xlabel('Days Since First Visit')
        plt.ylabel('Retention Rate (%)')
        plt.grid(True, alpha=0.3)
        plt.ylim(0, 105)  # 确保百分比显示完整
        
        # 标记关键天数的留存率
        key_days = [0, 1, 3, 7, 14, 30]
        for day in key_days:
            if day in retention_rates.index:
                plt.text(day, retention_rates[day] + 2, f'{retention_rates[day]:.1f}%', ha='center')
        
        # 创建输出目录
        os.makedirs("output", exist_ok=True)
        output_path = os.path.join("output", 'user_retention.png')
        plt.savefig(output_path, dpi=300)
        plt.close()
        logger.info(f"用户留存分析完成，已保存至: {output_path}")
    
    except Exception as e:
        logger.error(f"用户留存分析过程中发生错误: {str(e)}")
        logger.error(traceback.format_exc())


### 10. 主函数：整合所有任务 (添加大数据集优化)
def main():
    # 首先检查关键依赖
    logger.info("检查关键依赖...")
    try:
        import pymysql, pandas, seaborn, pyecharts
        logger.info("所有必需依赖已安装")
    except ImportError as e:
        missing_module = str(e).split(" ")[-1]
        logger.error(f"缺少关键依赖: {missing_module}")
        logger.error("请执行以下命令安装所有依赖:")
        logger.error("pip install pymysql pandas seaborn pyecharts psutil")
        return  # 退出程序

    parser = argparse.ArgumentParser(description='用户行为数据分析')
    parser.add_argument('--aggregate', action='store_true', help='使用预聚合查询优化大数据集')
    parser.add_argument('--chunk-size', type=int, default=100000, help='分块读取大小(默认100,000)')
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info(f"{'用户行为数据分析程序启动':^70}")
    logger.info(f"{'配置: ':<20} 预聚合={args.aggregate}, 分块大小={args.chunk_size}")
    logger.info("="*70)
    
    try:
        # 1. 获取数据
        logger.info(">>> 步骤1: 从数据库获取数据")
        data = get_data_from_mysql(
            use_aggregated_query=args.aggregate, 
            chunk_size=args.chunk_size
        )
        if data is None or data.empty:
            logger.error("数据获取失败，程序退出")
            return
        
        # 2. 预处理数据
        logger.info(">>> 步骤2: 数据预处理")
        processed_data = preprocess_data(data)
        if processed_data is None or processed_data.empty:
            logger.error("数据预处理失败，程序退出")
            return
        
        # 3. 执行各项可视化任务 (根据数据规模调整)
        logger.info(">>> 步骤3: 开始数据分析与可视化")
        
        # 创建任务列表
        analysis_tasks = [
            ("行为类型分布", plot_behavior_distribution),
            ("商品分类分析", plot_top_purchased_categories),
            ("月度行为分析", plot_monthly_behavior),
            ("省份购买分析", plot_province_purchase),
            ("每日行为趋势", plot_daily_behavior_trend),
            ("商品行为关联", plot_category_behavior_correlation),
            ("用户留存分析", plot_user_retention)
        ]
        
        # 大数据集跳过高成本任务
        if len(processed_data) > 5000000:  # 500万条以上
            logger.warning("检测到大数据集，跳过高成本分析任务")
            skip_tasks = ["商品行为关联", "用户留存分析"]
            analysis_tasks = [t for t in analysis_tasks if t[0] not in skip_tasks]
            logger.info(f"将执行的任务: {[t[0] for t in analysis_tasks]}")
        
        # 执行所有任务
        for task_name, task_func in analysis_tasks:
            try:
                logger.info(f"{'='*30} 开始任务: {task_name} {'='*30}")
                start_time = time.time()
                
                task_func(processed_data)
                
                elapsed = time.time() - start_time
                logger.info(f"{'='*30} 任务完成: {task_name} [耗时: {elapsed:.2f}秒] {'='*30}")
            except Exception as e:
                logger.error(f"任务 '{task_name}' 执行失败: {str(e)}")
                logger.error(traceback.format_exc())
                logger.info(f"跳过任务 '{task_name}'，继续执行后续任务")
        
        logger.info("="*70)
        logger.info(f"{'所有数据分析任务完成':^70}")
        logger.info("="*70)
    
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
    
    except Exception as e:
        logger.error(f"主程序发生未捕获的异常: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("程序异常终止")
    
    finally:
        # 程序结束前的清理工作
        plt.close('all')  # 关闭所有matplotlib图形
        logger.info("程序执行结束")


if __name__ == "__main__":
    # 添加系统资源监控
    logger.info(f"系统内存总量: {psutil.virtual_memory().total / (1024 ** 3):.2f} GB")
    logger.info(f"CPU核心数: {psutil.cpu_count()}")
    logger.info(f"Python版本: {sys.version}")
    
    main()