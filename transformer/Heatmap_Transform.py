from transformer.Data_Transform import Data_Transform
import pandas as pd
from Utils import filter_data, gen_fields_type, df_for_list
import json

class Heatmap_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "rect"
        self.fields_dict = gen_fields_type(data_file_url)
        self.template = """
{
  "dataset": [
    {
      "id": "init",
      "dimensions": ["b"],
      "source": [
        [143],
        [214],
        [251],
        [26],
        [86],
        [93],
        [176],
        [39],
        [221],
        [188],
        [57],
        [191],
        [8],
        [196],
        [177],
        [153]
      ]
    },
    {
      "transform": {
        "id": "histogram_data",
        "fromDatasetId": "init",
        "type": "ecStat:histogram",
        "config": {
          "dimensions": "b"
        }
      }
    }
  ],
  "tooltip": {},
  "xAxis": [
    {
      "type": "category"
    }
  ],
  "yAxis": [
    {
      "type": "value"
    }
  ],
  "series": [
    {
      "name": "histogram",
      "type": "bar",
      "label": {
        "show": "true",
        "position": "top"
      },
      "datasetIndex": 1
    }
  ]
}
"""
    # 可能会有x,y轴都是temporal
    def judge_temporal(self):
        if self.fields_dict[self.encoding["x"]] == "temporal" and self.fields_dict[self.encoding["y"]] != "temporal":
            return {"temporal": self.encoding["x"], "no_temporal": self.encoding["y"]}
        elif self.fields_dict[self.encoding["y"]] == "temporal" and self.fields_dict[self.encoding["x"]] != "temporal":
            return {"temporal": self.encoding["y"], "no_temporal": self.encoding["x"]}
        else:
            return None

    def transform(self):
        df = pd.read_csv(self.file_url)
        df = filter_data(df, self.filter)
        merged_df = pd.DataFrame()
        if not self.judge_temporal():
            if self.aggregate == "none":
                merged_df = df.groupby([self.encoding["x"], self.encoding["y"]]).agg(
                    {self.encoding["color"]: "sum"}).reset_index()
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
                grouped_df = df.groupby([pd.Grouper(key=self.judge_temporal()['temporal'], freq='D'),
                                         self.judge_temporal()['no_temporal']])[
                    self.encoding["color"]].sum().reset_index()
                grouped_df[self.judge_temporal()['temporal']] = grouped_df[
                    self.judge_temporal()['temporal']].dt.strftime('%Y-%m-%d')
            elif self.aggregate["aggregate"] == "count":
                grouped_df = df.groupby(
                    [self.judge_temporal()['temporal'], self.judge_temporal()['no_temporal']]).size().reset_index(
                    name='count')
                grouped_df[self.judge_temporal()['temporal']] = grouped_df[
                    self.judge_temporal()['temporal']].dt.strftime('%Y-%m-%d')
                # 按月进行分组
                merged_df = grouped_df.groupby(self.judge_temporal()['temporal'])["count"].sum().reset_index()
            else:
                merged_df = df.groupby([pd.Grouper(key=self.judge_temporal()['temporal'], freq='D'),self.judge_temporal()['no_temporal']]).agg({self.aggregate["field"]: self.aggregate["aggregate"]}).reset_index()
                merged_df[self.judge_temporal()['temporal']] = merged_df[self.judge_temporal()['temporal']].dt.strftime(
                    '%Y-%m-%d')
        return df_for_list(merged_df)

    def generate(self):
        data_list = self.transform()
        template_dict = json.loads(self.template)
        fields_name = data_list[0]
        data_list = data_list[1:]
        dimensions_list = []

        # -----------------------------------------设置 dataset---------------------------------------
        for field in fields_name:
            # 后续可以尝试使用 echarts 的time类型，只不过就不能在转换数据的时候进行时间分组
            if field == "count":
                dimensions_list.append({"name": "count", "type": "number"})
                continue
            if self.fields_dict[field] == "ordinal" or self.fields_dict[field] == "temporal" or self.fields_dict[field] == "nominal":
                dimensions_list.append({"name": field, "type": "ordinal"})
            else:
                dimensions_list.append({"name": field, "type": "number"})
        # 设置 dataset.dimensions
        template_dict['dataset'][0]['dimensions'] = dimensions_list
        # 设置 dataset.source
        template_dict['dataset'][0]['source'] = data_list
        if self.sort != "none":
            if self.sort[0] == "x":
                transform = [{"type": 'sort', "config": {"dimension": fields_name[0], "order": self.sort[1]}}]
            else:
                transform = [{"type": 'sort', "config": {"dimension": fields_name[1], "order": self.sort[1]}}]
            template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'init', "transform": transform})
        # -----------------------------------------设置 visualMap---------------------------------------
        numbers = [item for sublist in data_list for item in sublist if isinstance(item, (int, float))]
        max_value = max(numbers) if numbers else None
        min_value = min(numbers) if numbers else None
        template_dict['visualMap']['min'] = min_value
        template_dict['visualMap']['max'] = max_value
        # -----------------------------------------设置 series---------------------------------------
        # 后续增加图像名称命名
        if self.sort != "none":
            template_dict['series'][0]['datasetIndex'] = 1
        else:
            template_dict['series'][0]['datasetIndex'] = 0
        template_dict['series'][0]['encode']['x'] = fields_name[0]
        template_dict['series'][0]['encode']['y'] = fields_name[1]
        if aggregate != "none" and aggregate.split()[0] == "count":
            template_dict['series'][0]['encode']['value'] = "count"
        else:
            template_dict['series'][0]['encode']['value'] = fields_name[2]
        return template_dict


if __name__ == '__main__':
    data_file_url = r"../spider_csv/culture_company_movie.csv"
    aggregate = "count Budget_million,Gross_worldwide"
    encodings = "x=Budget_million,y=Gross_worldwide,color=count Budget_million and Gross_worldwide,size=none"
    filter = "Budget_million > 20 and Gross_worldwide > 1000000"
    trans = Heatmap_Transform(data_file_url, filter, aggregate, encodings)
    print(trans.transform())
