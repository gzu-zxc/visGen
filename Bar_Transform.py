from Data_Transform import Data_Transform
import pandas as pd
from summarizer import summarize
from Deepseek_llm import DeepSeekTextGenerator
import json
from Utils import filter_data


class Bar_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, aggregate, encoding, filter)
        self.fields_dict = self.data_summary()
        self.mark = "bar"
        self.group = self.judge_group()

    def data_summary(self):
        textgen = DeepSeekTextGenerator()
        summary = summarize(textgen, self.file_url)
        fields = summary['fields']
        fields_dict = dict()
        for field in fields:
            fields_dict[field['column']] = field["properties"]['visualization_type']
        return fields_dict

    def judge_group(self):

        # 后续增加数据summary进行groupby字段判断分组字段
        if self.aggregate != "none":
            if len(self.encoding['x'].split()) > 1:
                return {"groupby": self.encoding["y"], "grouped": self.encoding["x"].split()[1]}
            else:
                return {"groupby": self.encoding["x"], "grouped": self.encoding["y"].split()[1]}
        else:
            if self.fields_dict[self.encoding["x"]] == "nominal" and self.fields_dict[
                self.encoding["x"]] == "temporal" and self.fields_dict[self.encoding["x"]] == "ordinal":
                return {"groupby": self.encoding["x"], "grouped": self.encoding["y"]}
            else:
                return {"groupby": self.encoding["y"], "grouped": self.encoding["x"]}

    def transform(self):
        df = pd.read_csv(self.file_url)
        #对数据进行过滤
        df = filter_data(df, self.filter)
        unique_color = ""
        merged_df = pd.DataFrame()
        # 判断 groupby的类型是否为 "temporal"
        if self.fields_dict[self.group["groupby"]] == "temporal":
            # 将 groupby 列的时间转换为时间戳
            df[self.group['groupby']] = pd.to_datetime(df[self.group['groupby']])
            if self.encoding["color"] != "none":
                unique_color = df[self.encoding["color"]].unique()
                dfs = []
                for field in unique_color:
                    filtered_df = df[df[self.encoding["color"]] == field]
                    if self.aggregate == "none":
                        grouped_df = filtered_df.groupby(pd.Grouper(key=self.group['groupby'], freq='M'))[
                            self.group['grouped']].sum().reset_index()
                    elif self.aggregate["aggregate"] == "count":
                        grouped_df = filtered_df[self.group['groupby']].value_counts().reset_index()
                        grouped_df.columns = [self.group['groupby'], "count"]
                        # 按月进行分组
                        grouped_df = grouped_df.groupby(pd.Grouper(key=self.group['groupby'], freq='M'))[
                            "count"].sum().reset_index()
                    else:
                        grouped_df = filtered_df.groupby(pd.Grouper(key=self.group['groupby'], freq='M')).agg({
                            self.group['grouped']: self.aggregate["aggregate"]
                        })
                    grouped_df.columns = [self.group['groupby'], field]
                    dfs.append(grouped_df)
                merged_df = dfs.pop(0)
                for df_temp in dfs:
                    merged_df = pd.merge(merged_df, df_temp, on=merged_df.columns[0], how='outer')
        else:
            if self.encoding["color"] != "none":
                unique_color = df[self.encoding["color"]].unique()
                dfs = []
                for field in unique_color:
                    filtered_df = df[df[self.encoding["color"]] == field]
                    if self.aggregate == "none":
                        grouped_df = filtered_df.groupby(self.group['groupby']).agg({
                            self.group['grouping']: "sum"
                        })
                    elif self.aggregate["aggregate"] == "count":
                        grouped_df = filtered_df[self.group['groupby']].value_counts().reset_index()

                    else:
                        grouped_df = filtered_df.groupby(self.group['groupby']).agg({
                            self.aggregate["field"]: self.aggregate["aggregate"]
                        })
                    grouped_df.columns = [self.group['groupby'], field]
                    dfs.append(grouped_df)
                merged_df = dfs.pop(0)
                for df_temp in dfs:
                    merged_df = pd.merge(merged_df, df_temp, on=merged_df.columns[0], how='outer')
            else:
                if self.aggregate == "none":
                    # 当 aggregate 为 none 时，就将其聚合为sum
                    merged_df = df.groupby(self.group['groupby']).agg({
                        self.group['grouped']: "sum"
                    })
                    # 当 aggregate 为 count 时
                elif self.aggregate["aggregate"] == "count":
                    merged_df = df[self.group['groupby']].value_counts().reset_index()
                    merged_df.columns = [self.group['groupby'], "count"]
                else:
                    # 当 aggregate 为 其他时
                    merged_df = df.groupby(self.group['groupby']).agg({
                        self.group['grouped']: self.aggregate["aggregate"]
                    })
            merged_df.fillna(0, inplace=True)
        return merged_df


if __name__ == '__main__':
    data_file_url = r"C:\gitplace\TinyAgent\spider_csv\college_2_instructor.csv"
    aggregate = "mean salary"
    encodings = "x=dept_name,y=mean salary,color=none,size=none"
    trans = Bar_Transform(data_file_url, aggregate, encodings)
    print(trans.transform())
    trans.transform().to_csv('output_selected_columns.csv')
    # print(trans.data_summary())
