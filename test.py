import pandas as pd

# 创建一个示例DataFrame
df = pd.DataFrame({
    'A': [1, 2, 5, 6],
    'B': [4, 5, 6, 4],
    'C': ['a', 'b', 'c', 'a']
})
merged_df = df[df['C'] == 'a']
a = merged_df['B'].tolist()
a.insert(0,'a')
print(a)

print(merged_df)
# 将DataFrame的每一行转换为列表，并将这些列表存储在一个列表中
# list_of_columns = merged_df.columns.tolist()
list_of_rows = merged_df['A'].tolist()
list_of_rows.insert(0, 'A')

print(list_of_rows)
