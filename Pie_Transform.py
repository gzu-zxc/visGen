from Data_Transform import Data_Transform
import pandas as pd


class Pie_Transform(Data_Transform):
    def __init__(self, file_url: str, aggregate: str, encoding: str):
        super().__init__(file_url, aggregate, encoding)
        self.mark = "arc"

    def transform(self):
        df = pd.read_csv(self.file_url)
        unique_color = ""
        merged_df = pd.DataFrame()
        if self.aggregate == "none":
            merged_df = df.groupby(self.encoding["x"]).agg({
                self.aggregate["field"]: "sum"
            })
        elif self.aggregate["aggregate"] == "count":
            merged_df = df[self.encoding["x"]].value_counts().reset_index()
            merged_df.columns = [self.encoding["x"], "count"]
        else:
            merged_df = df.groupby(self.encoding["x"]).agg({
                self.aggregate["field"]: self.aggregate["aggregate"]
            })
        merged_df.fillna(0, inplace=True)
        return merged_df
