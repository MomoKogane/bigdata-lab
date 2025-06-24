#!/usr/bin/env python
import happybase
import csv

# 连接HBase
connection = happybase.Connection('localhost')
table = connection.table('user_action')

# 读取TSV文件
with open('user_action.tsv', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    batch = table.batch(batch_size=1000)
    
    for row in reader:
        row_key = row[0]
        data = {
            'f1:uid': row[1],
            'f1:item_id': row[2],
            'f1:behavior_type': row[3],
            'f1:item_category': row[4],
            'f1:visit_date': row[5],
            'f1:province': row[6]
        }
        batch.put(row_key, data)
    
    batch.send()

connection.close()