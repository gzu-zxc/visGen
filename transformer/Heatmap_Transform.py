from Data_Transform import Data_Transform
import pandas as pd
from Utils import filter_data, gen_fields_type,df_for_list


class Heatmap_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "rect"

    def transform(self):
        df = pd.read_csv(self.file_url)
        df = filter_data(df, self.filter)
        merged_df = pd.DataFrame()
        if self.aggregate == "none":
            merged_df = df.groupby([self.encoding["x"], self.encoding["y"]]).agg({self.encoding["color"]: "sum"}).reset_index()
        else:
            if self.aggregate["aggregate"] == "count":
                merged_df = df.groupby(["day_of_week", "station_id"]).size().reset_index()
                merged_df = merged_df.rename(columns={merged_df.columns[2]: 'count'})
            else:
                merged_df = df.groupby([self.encoding["x"], self.encoding["y"]]).agg(
                    {self.encoding["color"]: self.aggregate["aggregate"]}).reset_index()
        return df_for_list(merged_df)