class Data_Transform():
    def __init__(self, file_url: str, aggregate: str, encoding: str):
        self.file_url = file_url
        self.aggregate = self.transform_aggregate(aggregate)
        self.encoding = self.transform_encoding(encoding)

    def transform_aggregate(self, aggregate):
        if aggregate == 'none':
            return "none"
        words = aggregate.split()
        return {"aggregate": words[0], "field": words[1]}

    def transform_encoding(self, encoding):
        pairs = [pair.split('=') for pair in encoding.split(',')]
        resulting_dict = {key.strip(): value.strip() for key, value in pairs}
        return resulting_dict

    def transform_sort(self, sort):
        words = sort.split()
        return {"position": words[0], "direct": words[1]}

    def transform(self):
        pass
