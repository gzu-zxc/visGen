from Data_Transform import Data_Transform
import pandas as pd
from Utils import gen_fields_type, filter_data,df_for_list
import json
import copy

# Line 与 Area 的数据转换相同
class Line_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str, sort:str):
        super().__init__(data_file_url, filter, aggregate, encoding,sort)
        self.mark = "line"
        self.fields_dict = gen_fields_type(data_file_url)
        self.group = self.judge_group()
        self.templates = """
{
  "dataset": [
    'tooltip':{},
    {
    "id":"initial",
    "dimensions":["name",
        "good",
        "open",
        "close",
        "highest",
        "lowest"],
    "source": [
      ["a", 12, 44, 55, 66, 2],
      ["b", 23, 6, 16, 23, 1],
      ["c", 30, 6, 16, 23, 1]
    ]
  }],
  "legend": {},
  "yAxis": {
    "type":"value"
  },
  "xAxis": {
    "type": "category"
  },
  "series": [
    {
      "name":"open",
      "datasetIndex":0,
      "type": "line",
      "encode": {
        "x": "name",
        "y": "open"
      }
    }
  ]
}
"""

    def judge_group(self):
        # line 暂时只认为 temporal 类型为 groupby
        if self.fields_dict[self.encoding["x"]] == "temporal":
            return {"groupby": self.encoding["x"], "grouped": self.encoding["y"].split()[1]}
        else:
            return {"groupby": self.encoding["y"], "grouped": self.encoding["x"].split()[1]}

    def transform(self):
        df = pd.read_csv(self.file_url)
        df = filter_data(df, self.filter)
        if self.encoding['color'] == "none":
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
        else:
            unique_color = df[self.encoding["color"]].unique()
            df[self.group['groupby']] = pd.to_datetime(df[self.group['groupby']])
            dfs = []
            for field in unique_color:
                filtered_df = df[df[self.encoding["color"]] == field]
                print(filtered_df)
                print(type(filtered_df))
                print(self.group)
                if self.aggregate == "none":
                    grouped_df = filtered_df.groupby(pd.Grouper(key=self.group['groupby'], freq='D'))[
                        self.group['grouped']].sum().reset_index()
                    grouped_df[self.group['groupby']] = grouped_df[self.group['groupby']].dt.strftime('%Y-%m-%d')
                elif self.aggregate["aggregate"] == "count":
                    grouped_df = filtered_df[self.group['groupby']].value_counts().reset_index()
                    grouped_df[self.group['groupby']] = grouped_df[self.group['groupby']].dt.strftime('%Y-%m-%d')
                    # 按月进行分组
                    grouped_df = grouped_df.groupby(self.group['groupby'])["count"].sum().reset_index()
                else:
                    grouped_df = filtered_df.groupby(pd.Grouper(key=self.group['groupby'], freq='D')).agg(
                        {self.group['grouped']: self.aggregate["aggregate"]}).reset_index()
                    grouped_df[self.group['groupby']] = grouped_df[self.group['groupby']].dt.strftime('%Y-%m-%d')
                grouped_df.columns = [self.group['groupby'], field]
                dfs.append(grouped_df)
            merged_df = dfs.pop(0)
            for df_temp in dfs:
                merged_df = pd.merge(merged_df, df_temp, on=merged_df.columns[0], how='outer')
        merged_df.fillna(0, inplace=True)
        return df_for_list(merged_df)

    def generate(self):
        data_list = self.transform()
        template_dict = json.loads(self.templates)
        fields_name = data_list[0]
        data_list = data_list[1:]
        dimensions_list = []
        if self.encoding["color"] == "none":
            # -----------------------------------------设置 dataset---------------------------------------
            for field in fields_name:
                if field == "count":
                    dimensions_list.append({"name": "count", "type": "number"})
                    continue
                if self.fields_dict[field] == "ordinal" or self.fields_dict[field] == "temporal" or self.fields_dict[field] == "nominal":
                    dimensions_list.append({"name": field, "type": "ordinal"})
                else:
                    dimensions_list.append({"name": field, "type": "number"})
            template_dict['dataset'][0]['dimensions'] = dimensions_list
            template_dict['dataset'][0]['source'] = data_list
            if self.sort != "none":
                transform = [{"type": 'sort', "config": {"dimension": self.sort["position"], "order": self.sort["direct"]}}]
                template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'init', "transform": transform})
            # -----------------------------------------设置 series---------------------------------------
            if self.sort != "none":
                template_dict['series'][0]['datasetIndex'] = 1
            else:
                template_dict['series'][0]['datasetIndex'] = 0
            template_dict['series'][0]['name'] = fields_name[1]
            template_dict['series'][0]['encode']['x'] = fields_name[0]
            template_dict['series'][0]['encode']['y'] = fields_name[1]
        else:
            # -----------------------------------------设置 dataset---------------------------------------
            template_dict['dataset'][0]['source'] = data_list
            template_dict['dataset'][0]['dimensions'] = fields_name
            if self.sort != "none":
                transform = [{"type": 'sort', "config": {"dimension": self.sort["position"], "order": self.sort["direct"]}}]
                template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'init', "transform": transform})
            # -----------------------------------------设置 series---------------------------------------
            temp = copy.deepcopy(template_dict['series'][0])

            template_dict['series'][0]['name'] = fields_name[1]
            template_dict['series'][0]['encode']['x'] = fields_name[0]
            template_dict['series'][0]['encode']['y'] = fields_name[1]
            if self.sort != "none":
                template_dict['series'][0]['datasetIndex'] = 1
            else:
                template_dict['series'][0]['datasetIndex'] = 0

            for field in fields_name[2:]:
                temp['name'] = field
                temp['encode']['x'] = fields_name[0]
                temp['encode']['y'] = field
                if self.sort != "none":
                    temp['datasetIndex'] = 1
                else:
                    temp['datasetIndex'] = 0
                template_dict['series'].append(temp)
        return template_dict


                
if __name__ == '__main__':
    data_file_url = r"../spider_csv/behavior_monitoring_Student_Addresses.csv"
    aggregate = "mean monthly_rental"
    encodings = "x=date_address_to,y=mean monthly_rental,color=other_details,size=none"
    filter = "none"
    sort= "none"
    trans = Line_Transform(data_file_url, filter,aggregate, encodings,sort)
    print(trans.line_gen())
