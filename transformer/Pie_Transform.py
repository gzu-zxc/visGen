from transformer.Data_Transform import Data_Transform
import pandas as pd
from Utils import filter_data,df_for_list,gen_fields_type
import json


class Pie_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "arc"
        self.fields_dict = gen_fields_type(data_file_url)
        self.template = """
{
  "legend": {},
  "tooltip": {},
  "dataset": [{
    "id": "init",
    "dimensions": ["product", "2012", "2013", "2014", "2015", "2016", "2017"],
    "source": [
      ["Milk Tea", 86.5, 92.1, 85.7, 83.1, 73.4, 55.1],
      ["Matcha Latte", 41.1, 30.4, 65.1, 53.3, 83.8, 98.7],
      ["Cheese Cocoa", 24.1, 67.2, 79.5, 86.4, 65.2, 82.5],
      ["Walnut Brownie", 55.2, 67.1, 69.2, 72.4, 53.9, 39.1]
    ]
  }],
  "series": [
    {
      "datasetIndex": 0,
      "type": "pie",
      "encode": {
        "itemName": "product",
        "value": "2012"
      }
    }
  ]
}
"""
    # pie 默认 x轴是类别，y轴是值
    def transform(self):
        df = pd.read_csv(self.file_url)
        # 对数据进行过滤
        df = filter_data(df, self.filter)
        merged_df = pd.DataFrame()
        if self.aggregate == "none":
            merged_df = df.groupby(self.encoding["x"]).agg({
                self.encoding["y"]: "sum"
            }).reset_index()
        elif self.aggregate["aggregate"] == "count":
            merged_df = df[self.encoding["x"]].value_counts().reset_index()
        else:
            merged_df = df.groupby(self.encoding["x"]).agg({
                self.encoding["y"].split()[1]: self.aggregate["aggregate"]
            }).reset_index()
        print(merged_df)
        merged_df.fillna(0, inplace=True)
        return df_for_list(merged_df)

    def generate(self):
        data_list = self.transform()
        template_dict = json.loads(self.template)
        fields_name = data_list[0]
        data_list = data_list[1:]
        dimensions_list = []

        # -----------------------------------------设置 dataset---------------------------------------
        for field in fields_name:
            # 后续可以尝试使用 echarts 的time类型，只不过就不能在转换数据的时候进行时间分组
            if field == "count":
                dimensions_list.append({"name": "count", "type": "number"})
                continue
            if self.fields_dict[field] == "ordinal" or self.fields_dict[field] == "temporal" or self.fields_dict[field] == "nominal":
                dimensions_list.append({"name": field, "type": "ordinal"})
            else:
                dimensions_list.append({"name": field, "type": "number"})

            # 设置 dataset.dimensions
            template_dict['dataset'][0]['dimensions'] = dimensions_list
            # 设置 dataset.source
            template_dict['dataset'][0]['source'] = data_list
            # Pie 没有排序
            # -----------------------------------------设置 series---------------------------------------
            template_dict['series'][0]['encode']['itemName'] = fields_name[0]
            template_dict['series'][0]['encode']['value'] = fields_name[1]

        return template_dict

if __name__ == '__main__':
    data_file_url = r"../spider_csv/cre_Docs_and_Epenses_Documents.csv"
    aggregate = "count Document_Type_Code"
    encodings = "x=Document_Type_Code,y=count Document_Type_Code,color=none,size=none"
    filter = "none"
    trans = Pie_Transform(data_file_url, filter,aggregate, encodings)
    print(trans.transform())

