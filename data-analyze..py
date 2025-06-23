import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyecharts import options as opts
from pyecharts.charts import Map
import numpy as np

# ȷ������������ʾ
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False  # ���������ʾ����


### 1. ����MySQL���ݿⲢ��ȡ����
def get_data_from_mysql():
    """��MySQL��ȡ�û���Ϊ����"""
    print("�����������ݿⲢ��ȡ����...")
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
        print(f"�ɹ���ȡ���ݣ�������״: {data.shape}")
        return data
    except Exception as e:
        print(f"���ݿ�����ʧ��: {e}")
        return None


### 2. ����Ԥ������
def preprocess_data(data):
    """ת���������͡���ȡ�·ݲ������ֵ"""
    if data is not None:
        print("���ڽ�������Ԥ����...")
        # ��鲢�����ֵ
        if data.isnull().sum().sum() > 0:
            null_counts = data.isnull().sum()
            print(f"���ݴ��ڿ�ֵ�����п�ֵ����:\n{null_counts[null_counts > 0]}")
            data = data.dropna()
            print(f"���Ƴ���ֵ��ʣ��������״: {data.shape}")
        
        # ת����Ϊ����Ϊ��ֵ��
        data['behavior_type_num'] = data['behavior_type'].astype(int)
        # ����������ȡ�·�
        data['month'] = data['visit_date'].str[5:7]
        # ��ȡ�����е���
        data['day'] = data['visit_date'].str[8:10]
        print("����Ԥ�������")
        return data
    print("����Ϊ�գ��޷�����Ԥ����")
    return None


### 3. ��������Ϊ�ֲ����ӻ���ֱ��ͼ��
def plot_behavior_distribution(data):
    """ʹ��matplotlib������Ϊ���ͷֲ�ֱ��ͼ"""
    if data is not None:
        print("���ڻ�����������Ϊ���ͷֲ�ֱ��ͼ...")
        plt.figure(figsize=(10, 6))
        sns.histplot(data['behavior_type_num'], kde=False, bins=4, color='lightblue')
        plt.title('��������Ϊ���ͷֲ�')
        plt.xlabel('��Ϊ���ͣ�1=�����4=����')
        plt.ylabel('Ƶ��')
        plt.xticks([1, 2, 3, 4])
        plt.grid(True, alpha=0.3)
        plt.savefig('behavior_distribution.png', dpi=300)
        plt.show()
        print("��������Ϊ���ͷֲ�ֱ��ͼ�������")


### 4. ������ǰʮ����Ʒ���ࣨ��״ͼ��
def plot_top_purchased_categories(data):
    """ʹ��seaborn���ƹ�����ǰʮ����Ʒ����"""
    if data is not None:
        print("���ڷ��������ƹ�����ǰʮ����Ʒ����...")
        # ɸѡ������Ϊ����Ϊ����=4��
        buy_data = data[data['behavior_type_num'] == 4].copy()
        # ͳ����Ʒ���๺�����
        category_count = buy_data['item_category'].value_counts().nlargest(10)
        
        plt.figure(figsize=(12, 7))
        sns.barplot(x=category_count.index, y=category_count.values, color='green')
        plt.title('������ǰʮ����Ʒ����')
        plt.xlabel('��Ʒ����ID')
        plt.ylabel('�������')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)
        
        # Ϊÿ�����������ֵ��ǩ
        for i, v in enumerate(category_count.values):
            plt.text(i, v + 100, f'{v}', ha='center', fontsize=9)
        
        plt.savefig('top_categories.png', dpi=300)
        plt.show()
        print("������ǰʮ����Ʒ������״ͼ�������")


### 5. ���·���������Ϊ������ֱ��ͼ��
def plot_monthly_behavior(data):
    """ʹ��seaborn����ֱ��ͼ����������Ϊ�ֲ�"""
    if data is not None:
        print("���ڻ��Ƹ��·���������Ϊ�ֲ�����ֱ��ͼ...")
        plt.figure(figsize=(14, 8))
        sns.histplot(
            data=data,
            x='behavior_type_num',
            col='month',
            bins=4,
            kde=False,
            color='lightgreen',
            col_wrap=3  # ÿ����ʾ3����ͼ
        )
        plt.suptitle('���·���������Ϊ�ֲ�', y=0.95, fontsize=14)
        plt.tight_layout()
        plt.savefig('monthly_behavior.png', dpi=300)
        plt.show()
        print("���·���������Ϊ�ֲ�����ֱ��ͼ�������")


### 6. ��ʡ�ݹ���������������ͼ���ӻ���
def plot_province_purchase(data):
    """ʹ��pyecharts���Ƹ�ʡ�ݹ�������ͼ"""
    if data is not None:
        print("���ڷ�����ʡ�ݹ����������Ƶ�ͼ...")
        # ��ȡ�������ݲ�ͳ��ʡ�ݹ�����
        buy_data = data[data['behavior_type_num'] == 4].copy()
        province_count = buy_data['province'].value_counts().reset_index()
        province_count.columns = ['province', 'count']
        
        # ת��Ϊpyecharts��Ҫ�����ݸ�ʽ
        map_data = [list(z) for z in zip(province_count['province'].tolist(), province_count['count'].tolist())]
        
        # ������ͼ
        (
            Map()
            .add("������", map_data, "china", is_map_symbol_show=False)
            .set_global_opts(
                title_opts=opts.TitleOpts(title="��ʡ�ݹ������ֲ�"),
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
        print("��ʡ�ݹ�������ͼ������ɣ�����ѱ���ΪHTML�ļ�")


### 7. ÿ���û���Ϊ���Ʒ���������ͼ��
def plot_daily_behavior_trend(data):
    """ʹ��matplotlib����ÿ�ո�����Ϊ����"""
    if data is not None:
        print("���ڷ���ÿ���û���Ϊ����...")
        # �����ں���Ϊ���ͷ���ͳ��
        daily_trend = data.groupby(['day', 'behavior_type_num'])['uid'].nunique().reset_index()
        daily_trend = daily_trend.pivot(index='day', columns='behavior_type_num', values='uid')
        
        plt.figure(figsize=(14, 7))
        for behavior in daily_trend.columns:
            plt.plot(daily_trend.index, daily_trend[behavior], marker='o', label=f'��Ϊ����{behavior}')
        
        plt.title('ÿ���û���Ϊ����')
        plt.xlabel('����')
        plt.ylabel('�û�������ȥ��uid��')
        plt.legend(title='��Ϊ����')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha='right')
        
        plt.savefig('daily_behavior_trend.png', dpi=300)
        plt.show()
        print("ÿ����Ϊ���Ʒ�����ɣ�����ѱ���")


### 8. ��Ʒ��������Ϊ���͹�������������ͼ��
def plot_category_behavior_correlation(data):
    """ʹ��seaborn������Ʒ��������Ϊ���͹�������ͼ"""
    if data is not None:
        print("���ڷ�����Ʒ��������Ϊ���͹���...")
        # ͳ��ÿ����Ʒ�����¸���Ϊ���͵�����
        category_behavior = data.groupby(['item_category', 'behavior_type_num'])['uid'].count().reset_index()
        # ת��Ϊ�����
        category_behavior_pivot = category_behavior.pivot(index='item_category', columns='behavior_type_num', values='uid')
        # �����ܵĿ�ֵ
        category_behavior_pivot = category_behavior_pivot.fillna(0)
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(category_behavior_pivot, annot=True, fmt='g', cmap='YlGnBu', cbar_kws={'label': '��Ϊ����'})
        plt.title('��Ʒ��������Ϊ���͹�������ͼ')
        plt.xlabel('��Ϊ����')
        plt.ylabel('��Ʒ����ID')
        
        plt.savefig('category_behavior_heatmap.png', dpi=300)
        plt.show()
        print("��Ʒ��������Ϊ����������ɣ�����ѱ���")


### 9. �û��������
def plot_user_retention(data):
    """�����û�������������ӻ�"""
    if data is not None:
        print("���ڽ����û��������...")
        # ����visit_dateΪ�û��״η������ڣ�ʵ��Ӧ���������ҵ�����״η���
        data['first_visit'] = data.groupby('uid')['visit_date'].transform('min')
        # �����û��״η�����������ʵ�ʱ���
        data['date_diff'] = pd.to_datetime(data['visit_date']) - pd.to_datetime(data['first_visit'])
        data['date_diff_days'] = data['date_diff'].dt.days
        
        # ���û���ʱ�����飬ͳ�������û�
        retention_data = data.groupby(['uid', 'date_diff_days'])['visit_date'].count().reset_index()
        retention_data = retention_data[retention_data['date_diff_days'] <= 30]  # ������30��������
        
        # ����������
        first_day_users = retention_data[retention_data['date_diff_days'] == 0]['uid'].nunique()
        if first_day_users == 0:
            print("û���㹻���û����ݽ����������")
            return
        
        retention_rates = retention_data.groupby('date_diff_days')['uid'].nunique() / first_day_users * 100
        
        plt.figure(figsize=(12, 6))
        plt.plot(retention_rates.index, retention_rates.values, marker='o', color='red')
        plt.axhline(y=50, color='gray', linestyle='--', alpha=0.5)
        plt.title('�û�30��������')
        plt.xlabel('�״η��ʺ������')
        plt.ylabel('������ (%)')
        plt.grid(True, alpha=0.3)
        
        # ��ǹؼ�������������
        for day in [1, 7, 14, 30]:
            if day in retention_rates.index:
                plt.text(day, retention_rates[day] + 2, f'{retention_rates[day]:.1f}%', ha='center')
        
        plt.savefig('user_retention.png', dpi=300)
        plt.show()
        print("�û����������ɣ�����ѱ���")


### 10. ��������������������
def main():
    print("="*50)
    print("�û���Ϊ���ݷ�����������")
    print("="*50)
    
    # 1. ��ȡ����
    data = get_data_from_mysql()
    if data is None or data.empty:
        print("���ݻ�ȡʧ�ܣ������˳�")
        return
    
    # 2. Ԥ��������
    data = preprocess_data(data)
    if data is None or data.empty:
        print("����Ԥ�����Ϊ�գ������˳�")
        return
    
    # 3. ִ�и�����ӻ�����
    plot_behavior_distribution(data)
    plot_top_purchased_categories(data)
    plot_monthly_behavior(data)
    plot_province_purchase(data)
    plot_daily_behavior_trend(data)
    plot_category_behavior_correlation(data)
    plot_user_retention(data)
    
    print("="*50)
    print("�������ݷ���������ɣ�����ѱ���")
    print("="*50)


if __name__ == "__main__":
    main()