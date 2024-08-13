from Data_Transform import Data_Transform
import pandas as pd
from Utils import gen_fields_type, filter_data


class Line_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "line"
        self.fields_dict = gen_fields_type(data_file_url)
        self.group = self.judge_group()

    def judge_group(self):
        # line 暂时只认为 temporal 类型为 groupby
        if self.fields_dict[self.encoding["x"]] == "temporal":
            return {"groupby": self.encoding["x"], "grouped": self.encoding["y"]}
        else:
            return {"groupby": self.encoding["y"], "grouped": self.encoding["x"]}

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
            merged_df = df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME'))[self.group['grouped']].count().reset_index()
            merged_df[self.group['groupby']] = merged_df[self.group['groupby']].dt.strftime('%Y-%m')
            merged_df = merged_df.groupby([self.group['groupby']]).agg({self.group['grouped']: "sum"})
        else:
            merged_df = df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME'))[self.group['grouped']].agg({self.group['grouped']: self.aggregate["aggregate"]}).reset_index()
            # 将时间戳转换为 %Y-%m 格式
            merged_df[self.group['groupby']] = merged_df[self.group['groupby']].dt.strftime('%Y-%m')
        return merged_df
