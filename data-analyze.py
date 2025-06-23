import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyecharts import options as opts
from pyecharts.charts import Map
import numpy as np

# 确保中文正常显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题


### 1. 连接MySQL数据库并获取数据
def get_data_from_mysql():
    """从MySQL获取用户行为数据"""
    print("正在连接数据库并获取数据...")
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='root',
            database='dblab'
        )
        query = "SELECT * FROM user_action"
        data = pd.read_sql(query, conn)
        conn.close()
        print(f"成功获取数据，数据形状: {data.shape}")
        return data
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None


### 2. 数据预处理函数
def preprocess_data(data):
    """转换数据类型、提取月份并处理空值"""
    if data is not None:
        print("正在进行数据预处理...")
        # 检查并处理空值
        if data.isnull().sum().sum() > 0:
            null_counts = data.isnull().sum()
            print(f"数据存在空值，各列空值数量:\n{null_counts[null_counts > 0]}")
            data = data.dropna()
            print(f"已移除空值，剩余数据形状: {data.shape}")
        
        # 转换行为类型为数值型
        data['behavior_type_num'] = data['behavior_type'].astype(int)
        # 从日期中提取月份
        data['month'] = data['visit_date'].str[5:7]
        # 提取日期中的日
        data['day'] = data['visit_date'].str[8:10]
        print("数据预处理完成")
        return data
    print("数据为空，无法进行预处理")
    return None


### 3. 消费者行为分布可视化（直方图）
def plot_behavior_distribution(data):
    """使用matplotlib绘制行为类型分布直方图"""
    if data is not None:
        print("正在绘制消费者行为类型分布直方图...")
        plt.figure(figsize=(10, 6))
        sns.histplot(data['behavior_type_num'], kde=False, bins=4, color='lightblue')
        plt.title('消费者行为类型分布')
        plt.xlabel('行为类型（1=浏览，4=购买）')
        plt.ylabel('频数')
        plt.xticks([1, 2, 3, 4])
        plt.grid(True, alpha=0.3)
        plt.savefig('behavior_distribution.png', dpi=300)
        plt.show()
        print("消费者行为类型分布直方图绘制完成")


### 4. 购买量前十的商品分类（柱状图）
def plot_top_purchased_categories(data):
    """使用seaborn绘制购买量前十的商品分类"""
    if data is not None:
        print("正在分析并绘制购买量前十的商品分类...")
        # 筛选购买行为（行为类型=4）
        buy_data = data[data['behavior_type_num'] == 4].copy()
        # 统计商品分类购买次数
        category_count = buy_data['item_category'].value_counts().nlargest(10)
        
        plt.figure(figsize=(12, 7))
        sns.barplot(x=category_count.index, y=category_count.values, color='green')
        plt.title('购买量前十的商品分类')
        plt.xlabel('商品分类ID')
        plt.ylabel('购买次数')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)
        
        # 为每个柱子添加数值标签
        for i, v in enumerate(category_count.values):
            plt.text(i, v + 100, f'{v}', ha='center', fontsize=9)
        
        plt.savefig('top_categories.png', dpi=300)
        plt.show()
        print("购买量前十的商品分类柱状图绘制完成")


### 5. 按月分析购买行为（分面直方图）
def plot_monthly_behavior(data):
    """使用seaborn分面直方图分析各月行为分布"""
    if data is not None:
        print("正在绘制各月份消费者行为分布分面直方图...")
        plt.figure(figsize=(14, 8))
        sns.histplot(
            data=data,
            x='behavior_type_num',
            col='month',
            bins=4,
            kde=False,
            color='lightgreen',
            col_wrap=3  # 每行显示3个子图
        )
        plt.suptitle('各月份消费者行为分布', y=0.95, fontsize=14)
        plt.tight_layout()
        plt.savefig('monthly_behavior.png', dpi=300)
        plt.show()
        print("各月份消费者行为分布分面直方图绘制完成")


### 6. 各省份购买欲望分析（地图可视化）
def plot_province_purchase(data):
    """使用pyecharts绘制各省份购买量地图"""
    if data is not None:
        print("正在分析各省份购买量并绘制地图...")
        # 提取购买数据并统计省份购买量
        buy_data = data[data['behavior_type_num'] == 4].copy()
        province_count = buy_data['province'].value_counts().reset_index()
        province_count.columns = ['province', 'count']
        
        # 转换为pyecharts需要的数据格式
        map_data = [list(z) for z in zip(province_count['province'].tolist(), province_count['count'].tolist())]
        
        # 创建地图
        (
            Map()
            .add("购买量", map_data, "china", is_map_symbol_show=False)
            .set_global_opts(
                title_opts=opts.TitleOpts(title="各省份购买量分布"),
                visualmap_opts=opts.VisualMapOpts(
                    min_=province_count['count'].min(),
                    max_=province_count['count'].max(),
                    range_text=["High", "Low"],
                    range_color=["lightblue", "red"],
                    orient="vertical",
                    pos_right="10%",
                    pos_top="center"
                )
            )
            .render("province_purchase_map.html")
        )
        print("各省份购买量地图绘制完成，结果已保存为HTML文件")


### 7. 每日用户行为趋势分析（折线图）
def plot_daily_behavior_trend(data):
    """使用matplotlib绘制每日各类行为趋势"""
    if data is not None:
        print("正在分析每日用户行为趋势...")
        # 按日期和行为类型分组统计
        daily_trend = data.groupby(['day', 'behavior_type_num'])['uid'].nunique().reset_index()
        daily_trend = daily_trend.pivot(index='day', columns='behavior_type_num', values='uid')
        
        plt.figure(figsize=(14, 7))
        for behavior in daily_trend.columns:
            plt.plot(daily_trend.index, daily_trend[behavior], marker='o', label=f'行为类型{behavior}')
        
        plt.title('每日用户行为趋势')
        plt.xlabel('日期')
        plt.ylabel('用户数量（去重uid）')
        plt.legend(title='行为类型')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha='right')
        
        plt.savefig('daily_behavior_trend.png', dpi=300)
        plt.show()
        print("每日行为趋势分析完成，结果已保存")


### 8. 商品分类与行为类型关联分析（热力图）
def plot_category_behavior_correlation(data):
    """使用seaborn绘制商品分类与行为类型关联热力图"""
    if data is not None:
        print("正在分析商品分类与行为类型关联...")
        # 统计每个商品分类下各行为类型的数量
        category_behavior = data.groupby(['item_category', 'behavior_type_num'])['uid'].count().reset_index()
        # 转换为交叉表
        category_behavior_pivot = category_behavior.pivot(index='item_category', columns='behavior_type_num', values='uid')
        # 填充可能的空值
        category_behavior_pivot = category_behavior_pivot.fillna(0)
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(category_behavior_pivot, annot=True, fmt='g', cmap='YlGnBu', cbar_kws={'label': '行为次数'})
        plt.title('商品分类与行为类型关联热力图')
        plt.xlabel('行为类型')
        plt.ylabel('商品分类ID')
        
        plt.savefig('category_behavior_heatmap.png', dpi=300)
        plt.show()
        print("商品分类与行为关联分析完成，结果已保存")


### 9. 用户留存分析
def plot_user_retention(data):
    """分析用户留存情况并可视化"""
    if data is not None:
        print("正在进行用户留存分析...")
        # 假设visit_date为用户首次访问日期，实际应用中需根据业务定义首次访问
        data['first_visit'] = data.groupby('uid')['visit_date'].transform('min')
        # 计算用户首次访问与后续访问的时间差
        data['date_diff'] = pd.to_datetime(data['visit_date']) - pd.to_datetime(data['first_visit'])
        data['date_diff_days'] = data['date_diff'].dt.days
        
        # 按用户和时间差分组，统计留存用户
        retention_data = data.groupby(['uid', 'date_diff_days'])['visit_date'].count().reset_index()
        retention_data = retention_data[retention_data['date_diff_days'] <= 30]  # 仅分析30天内留存
        
        # 计算留存率
        first_day_users = retention_data[retention_data['date_diff_days'] == 0]['uid'].nunique()
        if first_day_users == 0:
            print("没有足够的用户数据进行留存分析")
            return
        
        retention_rates = retention_data.groupby('date_diff_days')['uid'].nunique() / first_day_users * 100
        
        plt.figure(figsize=(12, 6))
        plt.plot(retention_rates.index, retention_rates.values, marker='o', color='red')
        plt.axhline(y=50, color='gray', linestyle='--', alpha=0.5)
        plt.title('用户30天留存率')
        plt.xlabel('首次访问后的天数')
        plt.ylabel('留存率 (%)')
        plt.grid(True, alpha=0.3)
        
        # 标记关键天数的留存率
        for day in [1, 7, 14, 30]:
            if day in retention_rates.index:
                plt.text(day, retention_rates[day] + 2, f'{retention_rates[day]:.1f}%', ha='center')
        
        plt.savefig('user_retention.png', dpi=300)
        plt.show()
        print("用户留存分析完成，结果已保存")


### 10. 主函数：整合所有任务
def main():
    print("="*50)
    print("用户行为数据分析程序启动")
    print("="*50)
    
    # 1. 获取数据
    data = get_data_from_mysql()
    if data is None or data.empty:
        print("数据获取失败，程序退出")
        return
    
    # 2. 预处理数据
    data = preprocess_data(data)
    if data is None or data.empty:
        print("数据预处理后为空，程序退出")
        return
    
    # 3. 执行各项可视化任务
    plot_behavior_distribution(data)
    plot_top_purchased_categories(data)
    plot_monthly_behavior(data)
    plot_province_purchase(data)
    plot_daily_behavior_trend(data)
    plot_category_behavior_correlation(data)
    plot_user_retention(data)
    
    print("="*50)
    print("所有数据分析任务完成，结果已保存")
    print("="*50)


if __name__ == "__main__":
    main()