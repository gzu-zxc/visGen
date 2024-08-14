from Data_Transform import Data_Transform
import pandas as pd
from Utils import gen_fields_type, filter_data,df_for_list


# Line 与 Area 的数据转换相同
class Line_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "line"
        self.fields_dict = gen_fields_type(data_file_url)
        self.group = self.judge_group()

    def judge_group(self):
        # line 暂时只认为 temporal 类型为 groupby
        if self.fields_dict[self.encoding["x"]] == "temporal":
            return {"groupby": self.encoding["x"], "grouped": self.encoding["y"].split()[1]}
        else:
            return {"groupby": self.encoding["y"], "grouped": self.encoding["x"].split()[1]}

    def transform(self):
        df = pd.read_csv(self.file_url)
        df = filter_data(df, self.filter)
        # 这里后续需要保证数据是能转换为时间戳
        df[self.group['groupby']] = pd.to_datetime(df[self.group['groupby']])
        if self.aggregate == "none":
            # 使用 pd.Grouper(freq='M') 来按月分组
            merged_df = df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME'))[self.group['grouped']].sum().reset_index()
            merged_df[self.group['groupby']] = merged_df[self.group['groupby']].dt.strftime('%Y-%m')
        elif self.aggregate["aggregate"] == "count":
            merged_df = df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME')).value_counts().reset_index()
            merged_df[self.group['groupby']] = merged_df[self.group['groupby']].dt.strftime('%Y-%m')
            merged_df = merged_df.groupby([self.group['groupby']]).agg({"count": "sum"}).reset_index()
        else:
            merged_df = df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME')).agg({self.group['grouped']: self.aggregate["aggregate"]}).reset_index()
            # 将时间戳转换为 %Y-%m 格式
            merged_df[self.group['groupby']] = merged_df[self.group['groupby']].dt.strftime('%Y-%m')
        return df_for_list(merged_df)

if __name__ == '__main__':
    data_file_url = r"../spider_csv/cre_Doc_Control_Systems_Documents.csv"
    aggregate = "count receipt_date"
    encodings = "x=receipt_date,y=count receipt_date,color=none,size=none"
    filter = "none"
    trans = Line_Transform(data_file_url, filter,aggregate, encodings)
    print(trans.transform())
