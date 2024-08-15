class Data_Transform():

    def __init__(self, file_url: str, filter: str, aggregate: str, encoding: str, sort:str):
        self.file_url = file_url
        self.aggregate = self.transform_aggregate(aggregate)
        self.encoding = self.transform_encoding(encoding)
        self.filter = filter
        self.sort = self.transform_sort(sort)

    def transform_aggregate(self, aggregate):
        if aggregate == 'none':
            return "none"
        words = aggregate.split()
        return {"aggregate": words[0], "field": words[1]}

    def transform_encoding(self, encoding):
        pairs = [pair.split('=') for pair in encoding.split(',')]
        # 这里是区分 heatmap count的情况
        if len(pairs) == 5:
            parts = encoding.split(',')
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
        resulting_dict = {key.strip(): value.strip() for key, value in pairs}
        return resulting_dict

    def transform_sort(self, sort):
        if sort == 'none':
            return "none"
        words = sort.split()
        return {"position": words[0], "direct": words[1]}

    def generate(self):
        pass

    def transform(self):
        pass
