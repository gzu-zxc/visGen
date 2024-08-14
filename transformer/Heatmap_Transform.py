from transformer.Data_Transform import Data_Transform
import pandas as pd
from Utils import filter_data, gen_fields_type,df_for_list


class Heatmap_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "rect"
        self.fields_dict = gen_fields_type(data_file_url)

    # 可能会有x,y轴都是temporal
    def judge_temporal(self):
        if self.fields_dict[self.encoding["x"]] == "temporal" and self.fields_dict[self.encoding["y"]] != "temporal":
            return {"temporal":self.encoding["x"],"no_temporal":self.encoding["y"]}
        elif self.fields_dict[self.encoding["y"]] == "temporal" and self.fields_dict[self.encoding["x"]] != "temporal":
            return {"temporal":self.encoding["y"],"no_temporal":self.encoding["x"]}
        else:
            return None
    def transform(self):
        df = pd.read_csv(self.file_url)
        df = filter_data(df, self.filter)
        merged_df = pd.DataFrame()
        if not self.judge_temporal():
            if self.aggregate == "none":
                merged_df = df.groupby([self.encoding["x"], self.encoding["y"]]).agg({self.encoding["color"]: "sum"}).reset_index()
            else:
                if self.aggregate["aggregate"] == "count":
                    merged_df = df.groupby([self.encoding["x"], self.encoding["y"]]).size().reset_index()
                    merged_df = merged_df.rename(columns={merged_df.columns[2]: 'count'})
                else:
                    merged_df = df.groupby([self.encoding["x"], self.encoding["y"]]).agg(
                        {self.aggregate["field"]: self.aggregate["aggregate"]}).reset_index()
        else:
            df[self.judge_temporal()['temporal']] = pd.to_datetime(df[self.judge_temporal()['temporal']])
            if self.aggregate == "none":
                grouped_df = df.groupby([pd.Grouper(key=self.judge_temporal()['temporal'], freq='D'),self.judge_temporal()['no_temporal']])[self.encoding["color"]].sum().reset_index()
                grouped_df[self.judge_temporal()['temporal']] = grouped_df[self.judge_temporal()['temporal']].dt.strftime('%Y-%m-%d')
            elif self.aggregate["aggregate"] == "count":
                grouped_df = df.groupby([self.judge_temporal()['temporal'], self.judge_temporal()['no_temporal']]).size().reset_index(name='count')
                grouped_df[self.judge_temporal()['temporal']] = grouped_df[self.judge_temporal()['temporal']].dt.strftime('%Y-%m-%d')
                # 按月进行分组
                merged_df = grouped_df.groupby(self.judge_temporal()['temporal'])["count"].sum().reset_index()
            else:
                merged_df = df.groupby([pd.Grouper(key=self.judge_temporal()['temporal'], freq='D'),self.judge_temporal()['no_temporal']]).agg({self.aggregate["field"]: self.aggregate["aggregate"]}).reset_index()
                merged_df[self.judge_temporal()['temporal']] = merged_df[self.judge_temporal()['temporal']].dt.strftime('%Y-%m-%d')
        return df_for_list(merged_df)

if __name__ == '__main__':
    data_file_url = r"../spider_csv/flight_1_Flight.csv"
    aggregate = "mean price"
    encodings = "x=origin,y=destination,color=mean price,size=none"
    filter = "none"
    trans = Heatmap_Transform(data_file_url, filter,aggregate, encodings)
    print(trans.transform())