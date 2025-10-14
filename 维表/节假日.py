import pandas as pd
from datetime import datetime, timedelta
import os

# 创建输出目录
output_path = "holiday_calendar_2025.csv"
# os.makedirs(os.path.dirname(output_path), exist_ok=True)

# 生成2025年所有日期
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 12, 31)
date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# 定义节假日和调休上班日
holidays = set([
    '2025-01-01',
    '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31', '2025-02-01', '2025-02-02', '2025-02-03', '2025-02-04',
    '2025-04-04', '2025-04-05', '2025-04-06',
    '2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04', '2025-05-05',
    '2025-05-31', '2025-06-01', '2025-06-02',
    '2025-10-01', '2025-10-02', '2025-10-03', '2025-10-04', '2025-10-05', '2025-10-06', '2025-10-07', '2025-10-08'
])

makeup_workdays = set([
    '2025-01-26', '2025-02-08', '2025-04-27', '2025-09-28', '2025-10-11'
])

# 构建表格数据
rows = []
for date in date_list:
    date_str = date.strftime('%Y-%m-%d')
    weekday = date.weekday()  # 0=Monday, 6=Sunday

    if date_str in holidays:
        label = '节假日'
    elif date_str in makeup_workdays:
        label = '工作日'
    elif weekday in [5, 6]:
        label = '周末'
    else:
        label = '工作日'

    rows.append({'日期': date_str, '日期标签': label})

# 保存为CSV
df = pd.DataFrame(rows)
df.to_csv(output_path, index=False)
print(df.head())
