import pandas as pd

from transformer.Data_Transform import Data_Transform
from Utils import filter_data, gen_fields_type
import json

class Boxplot_Transform(Data_Transform):

    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.fields_dict = gen_fields_type(data_file_url)
        self.mark = "boxplot"
        self.group = self.judge_group()
        self.template = """
{
  "title": [
    {
      "text": "boxplot",
      "left": "center"
    }
  ],
  "dataset": [
    {
      "id": "init",
      "source": [
        ["a", 850, 740, 900, 1070, 930, 850, 950, 980, 980, 880, 1000, 980, 930, 650, 760, 810, 1000, 1000, 960, 960],
        ["b", 960, 940, 960, 940, 880, 850, 880, 900, 840, 830, 790, 810, 880, 880, 830, 800, 790, 760, 800],
        ["c", 880, 880, 880, 860, 720, 720, 620, 860, 970, 950, 880, 910, 850, 870, 840, 840, 850, 840, 840, 840],
        ["d", 890, 810, 810, 710, 920, 890, 860, 880, 720, 840, 850, 850, 780],
        ["e", 890, 840, 780, 810, 760, 810, 790, 810, 820, 850, 870, 870, 810, 740, 810, 940, 950, 800, 810, 870]
      ]
    }
  ],
  "tooltip": {
    "trigger": "item",
    "axisPointer": {
      "type": "shadow"
    }
  },
  "grid": {
    "left": "10%",
    "right": "10%",
    "bottom": "15%"
  },
  "xAxis": {
    "type": "value"
  },
  "yAxis": {
    "type": "category"
  },
  "series": [
    {
      "name": "boxplot",
      "type": "boxplot",
      "encode": {
        "y": 0
      },
      "datasetIndex": 0
    }
  ]
}
"""

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

    def generate(self):
        data_list = self.transform()
        template_dict = json.loads(self.template)
        # -----------------------------------------设置 dataset---------------------------------------
        # 设置 dataset.source
        template_dict['dataset'][0]['source'] = data_list
        if self.sort != "none":
            transform = [{"type": 'sort', "config": {"dimension": 0, "order": self.sort[1]}}]
            template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'init', "transform": transform})
        # -----------------------------------------设置 series---------------------------------------
        template_dict['series'][0]['encode']['y'] = 0
        if self.sort != "none":
            template_dict['series'][0]["datasetIndex"] = 1
        return template_dict


if __name__ == '__main__':
    data_file_url = r"../spider_csv/culture_company_movie.csv"
    aggregate = "none"
    encodings = "x=none,y=Budget_million,color=none,size=none"
    filter = "Budget_million > 30"
    trans = Boxplot_Transform(data_file_url, filter, aggregate, encodings)
    print(trans.transform())
