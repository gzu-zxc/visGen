import pandas as pd

from Data_Transform import Data_Transform
from Utils import filter_data, gen_fields_type


class Boxplot_Transform(Data_Transform):

    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.fields_dict = gen_fields_type(data_file_url)
        self.mark = "boxplot"
        self.group = self.judge_group()

    def judge_group(self):
        if self.encoding["x"] == "none":
            return {"groupby": "none", "grouped": self.encoding["y"]}
        elif self.encoding["y"] == "none":
            return {"groupby": "none", "grouped": self.encoding["x"]}

        else:
            if self.aggregate != "none":
                if len(self.encoding['x'].split()) > 1:
                    return {"groupby": self.encoding["y"], "grouped": self.encoding["x"].split()[1]}
                else:
                    return {"groupby": self.encoding["x"], "grouped": self.encoding["y"].split()[1]}
            else:
                if self.fields_dict[self.encoding["x"]] == "nominal" or self.fields_dict[
                    self.encoding["x"]] == "temporal" or self.fields_dict[self.encoding["x"]] == "ordinal":
                    return {"groupby": self.encoding["x"], "grouped": self.encoding["y"]}
                else:
                    return {"groupby": self.encoding["y"], "grouped": self.encoding["x"]}

    def transform(self):
        df = pd.read_csv(self.file_url)
        # 对数据进行过滤
        df = filter_data(df, self.filter)
        data_lists = []
        if self.group["groupby"] == "none":
            data_list = df[[self.group["grouped"]]][self.group["grouped"]].tolist()
            data_list.insert(0, self.group["grouped"])
            data_lists.append(data_list)
        else:
            unique_field = df[self.group["groupby"]].unique()
            print(unique_field)
            for field in unique_field:
                filtered_df = df[df[self.group["groupby"]] == field]
                print(filtered_df)
                data_list = filtered_df[self.group["grouped"]].tolist()
                data_list.insert(0, field)
                data_lists.append(data_list)
        return data_lists


if __name__ == '__main__':
    data_file_url = r"../spider_csv/culture_company_movie.csv"
    aggregate = "none"
    encodings = "x=none,y=Budget_million,color=none,size=none"
    filter = "Budget_million > 30"
    trans = Boxplot_Transform(data_file_url, filter, aggregate, encodings)
    print(trans.transform())
