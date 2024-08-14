nested_list = [[1, "a", 3], [4, 5, "b"], ["c", 6, 7]]

# 提取所有数字
numbers = [item for sublist in nested_list for item in sublist if isinstance(item, (int, float))]

# 计算最大值和最小值
max_value = max(numbers) if numbers else None
min_value = min(numbers) if numbers else None

print("最大值:", max_value)
print("最小值:", min_value)
