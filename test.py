def parse_string(input_str):
    # 分割字符串，按逗号分隔不同参数
    parts = input_str.split(',')

    # 创建一个空的字典来存储解析后的数据
    result = {}

    for part in parts:
        # 检查等号是否存在
        if '=' in part:
            key, value = part.split('=', 1)
            # 如果字典中已存在相同的key，说明value包含多个值，用逗号拼接
            if key in result:
                result[key] += ',' + value
            else:
                result[key] = value
        else:
            # 如果没有等号，将这个部分作为上一个key的附加值
            # 这里假设这个部分是颜色的值
            result['color'] += ',' + part

    return result


input_str = "x=Budget_million,y=Gross_worldwide,color=count Budget_million,Gross_worldwide,size=none"
parsed_dict = parse_string(input_str)

print(parsed_dict)
