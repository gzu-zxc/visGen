from transformer.Data_Transform import Data_Transform
import pandas as pd
from Utils import filter_data,df_for_list


class Pie_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "arc"

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

if __name__ == '__main__':
    data_file_url = r"../spider_csv/cre_Docs_and_Epenses_Documents.csv"
    aggregate = "count Document_Type_Code"
    encodings = "x=Document_Type_Code,y=count Document_Type_Code,color=none,size=none"
    filter = "none"
    trans = Pie_Transform(data_file_url, filter,aggregate, encodings)
    print(trans.transform())

