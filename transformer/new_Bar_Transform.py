import copy
import json

from Data_Transform import Data_Transform
import pandas as pd
from LLM.summarizer import summarize
from LLM.Deepseek_llm import DeepSeekTextGenerator
from Utils import filter_data,gen_fields_type,df_for_list
class Bar_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str,sort):
        super().__init__(data_file_url, filter, aggregate, encoding,sort)
        self.fields_dict = gen_fields_type(data_file_url)
        self.mark = "bar"
        self.group = self.judge_group()
        self.encoding_str=encoding
        self.templates = """
    {
       "dataset": [
    {
    "id":"initial",
    "dimensions":[
        ],
    "source": [
    ]
  }],
  "yAxis": {
    "type":"value"
  },
  "xAxis": {
    "type": "category"
  },
  "tooltip":{
  "show":true
  },
  "legend":{
    "show":true
  },
  "series": [
    {
      "datasetId":"initial",
      "type": "bar",
      "encode": {
        "x": "name",
        "y": "open"
      }
    }
  ]
    }
    """
    def judge_group(self):

        # 后续增加数据summary进行groupby字段判断分组字段
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
                        grouped_df = filtered_df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME'))[self.group['grouped']].sum().reset_index()
                        grouped_df[self.group['groupby']] = grouped_df[self.group['groupby']].dt.strftime('%Y-%m')
                    elif self.aggregate["aggregate"] == "count":
                        grouped_df = filtered_df[self.group['groupby']].value_counts().reset_index()
                        grouped_df[self.group['groupby']] = grouped_df[self.group['groupby']].dt.strftime('%Y-%m')
                        # 按月进行分组
                        grouped_df = grouped_df.groupby(self.group['groupby'])["count"].sum().reset_index()
                    else:
                        grouped_df = filtered_df.groupby(pd.Grouper(key=self.group['groupby'], freq='ME')).agg({self.group['grouped']: self.aggregate["aggregate"]}).reset_index()
                        grouped_df[self.group['groupby']] = grouped_df[self.group['groupby']].dt.strftime('%Y-%m')
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
                        }).reset_index()
                    elif self.aggregate["aggregate"] == "count":
                        grouped_df = filtered_df[self.group['groupby']].value_counts().reset_index()
                    else:
                        grouped_df = filtered_df.groupby(self.group['groupby']).agg({self.aggregate["field"]: self.aggregate["aggregate"]}).reset_index()
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
                    }).reset_index()
                    # 当 aggregate 为 count 时
                elif self.aggregate["aggregate"] == "count":
                    merged_df = df[self.group['groupby']].value_counts().reset_index()
                    merged_df.columns = [self.group['groupby'], "count"]
                else:
                    # 当 aggregate 为 其他时
                    merged_df = df.groupby(self.group['groupby']).agg({
                        self.group['grouped']: self.aggregate["aggregate"]
                    }).reset_index()
            merged_df.fillna(0, inplace=True)

        return df_for_list(merged_df)
    def generate(self):
        data_list = self.transform()
        template_dict = json.loads(self.templates)
        fields_name = data_list[0]
        data_list = data_list[1:]
        dimensions_list = []
        #解析字符串
        pairs = self.encoding_str.split(',')
        encode = {}
        for pair in pairs:
            key, value = pair.split('=')
            key = key.strip()
            value = value.strip()
            encode[key] = value
        if self.encoding["color"] == "none":
            # -----------------------------------------设置 dataset---------------------------------------
            for field in fields_name:
                if field == "count":
                    dimensions_list.append({"name": "count", "type": "number"})
                    continue
                if self.fields_dict[field] == "ordinal" or self.fields_dict[field] == "temporal" or self.fields_dict[
                    field] == "nominal":
                    dimensions_list.append({"name": field, "type": "ordinal"})
                else:
                    dimensions_list.append({"name": field, "type": "number"})
            template_dict['dataset'][0]['dimensions'] = dimensions_list
            template_dict['dataset'][0]['source'] = data_list
            # -----------------------------------------设置 series---------------------------------------
            if self.sort != "none":
                template_dict['series'][0]['datasetIndex'] = 1
            else:
                template_dict['series'][0]['datasetIndex'] = 0
            template_dict['series'][0]['name'] = fields_name[1]
            template_dict['series'][0]['encode']['x'] = fields_name[0]
            template_dict['series'][0]['encode']['y'] = fields_name[1]
            #-------------根据encode设置排序轴
            if self.sort != "none":
                sort_dim = template_dict['series'][0]['encode'][self.sort["position"]]
                transform = [
                    {"type": 'sort', "config": {"dimension":sort_dim , "order": self.sort["direct"]}}]
                template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'initial', "transform": transform})
        else:#堆叠柱状图
            # -----------------------------------------设置 dataset---------------------------------------
            template_dict['dataset'][0]['source'] = data_list
            template_dict['dataset'][0]['dimensions'] = fields_name
            # -----------------------------------------设置 series---------------------------------------
            temp = copy.deepcopy(template_dict['series'][0])
            template_dict['series'][0]['name'] = fields_name[1]
            template_dict['series'][0]['encode']['x'] = fields_name[0]
            template_dict['series'][0]['encode']['y'] = fields_name[1]
            if self.sort != "none":
                template_dict['series'][0]['datasetIndex'] = 1
            else:
                template_dict['series'][0]['datasetIndex'] = 0
            #-------------添加多个series
            for field in fields_name[2:]:
                temp['name'] = field
                temp['encode']['x'] = fields_name[0]
                temp['encode']['y'] = field
                if self.sort != "none":
                    temp['datasetIndex'] = 1
                else:
                    temp['datasetIndex'] = 0
                template_dict['series'].append(temp)
            #-------设置transform----------
            if self.sort != "none":
                sort_dim = template_dict['series'][0]['encode'][self.sort['position']]
                transform = [
                    {"type": 'sort', "config": {"dimension": sort_dim, "order": self.sort["direct"]}}]
                template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'init', "transform": transform})
            #------------堆叠柱状图--------------
            for series in template_dict['series']:
                series['stack'] = 'total'
        return template_dict

if __name__ == '__main__':
    data_file_url = r"../spider_csv/aircraft_aircraft.csv"
    aggregate = "count Description"
    encodings = "x=Description,y=count Description,color=none,size=none"
    filter = "none"
    sort = "x asc"
    trans = Bar_Transform(data_file_url, filter, aggregate, encodings,sort)
    print(trans.transform())
    print(trans.generate())
    with open('echarts_config.json', 'w', encoding='utf-8') as f:
        json.dump(trans.generate(), f, ensure_ascii=False, indent=4)
        print("ECharts配置文件已生成：echarts_config.json")
