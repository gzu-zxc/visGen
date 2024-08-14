from transformer.Heatmap_Transform import Heatmap_Transform
import json
from Utils import gen_fields_type
from itertools import chain

template = """
{
  "dataset": [
    {
      "id": "init",
      "dimensions": ["a", "b", "num"],
      "source": [
        ["a0", "b0", 1],
        ["a0", "b1", 2],
        ["a1", "b0", 3],
        ["a1", "b1", 8],
        ["a1", "b2", 3.2],
        ["a2", "b3", 4.9]
      ]
    }
  ],
  "tooltip": {
    "position": "top"
  },
  "grid": {
    "height": "50%",
    "top": "10%"
  },
  "xAxis": {
    "type": "category"
  },
  "yAxis": {
    "type": "category"
  },
  "visualMap": {
    "min": 1,
    "max": 8,
    "calculable": "true",
    "orient": "horizontal",
    "left": "center",
    "bottom": "15%"
  },
  "series": [
    {
      "name": "Punch Card",
      "type": "heatmap",
      "datasetIndex": 0,
      "encode": {
        "x": "a",
        "y": "b",
        "value": "num"
      },
      "label": {
        "show": "true"
      }
    }
  ]
}
"""


def heatmap_gen(file_url, filter, aggregate, encoding, sort):
    fields_type = gen_fields_type(file_url)
    trans = Heatmap_Transform(file_url, filter, aggregate, encoding)
    data_list = trans.transform()
    template_dict = json.loads(template)
    fields_name = data_list[0]
    data_list = data_list[1:]
    dimensions_list = []

    #-----------------------------------------设置 dataset---------------------------------------
    for field in fields_name:
        # 后续可以尝试使用 echarts 的time类型，只不过就不能在转换数据的时候进行时间分组
        if field == "count":
            dimensions_list.append({"name": "count", "type": "number"})
            continue
        if fields_type[field] == "ordinal" or fields_type[field] == "temporal" or fields_type[field] == "nominal":
            dimensions_list.append({"name": field, "type": "ordinal"})
        else:
            dimensions_list.append({"name": field, "type": "number"})
    # 设置 dataset.dimensions
    template_dict['dataset'][0]['dimensions'] = dimensions_list
    # 设置 dataset.source
    template_dict['dataset'][0]['source'] = data_list
    if sort != "none":
        sort_list = sort.split()
        if sort_list[0] == "x":
            transform = [{"type": 'sort', "config": {"dimension": fields_name[0], "order": sort_list[1]}}]
        else:
            transform = [{"type": 'sort', "config": {"dimension": fields_name[1], "order": sort_list[1]}}]
        template_dict['dataset'].append({"id": "sort_data", "fromDatasetId": 'init', "transform": transform})
    # -----------------------------------------设置 visualMap---------------------------------------
    numbers = [item for sublist in data_list for item in sublist if isinstance(item, (int, float))]
    max_value = max(numbers) if numbers else None
    min_value = min(numbers) if numbers else None
    template_dict['visualMap']['min'] = min_value
    template_dict['visualMap']['max'] = max_value
    # -----------------------------------------设置 series---------------------------------------
    # 后续增加图像名称命名
    if sort != "none":
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
    data_file_url = r"../spider_csv/flight_1_Flight.csv"
    aggregate = "mean price"
    encodings = "x=origin,y=destination,color=mean price,size=none"
    filter = "none"
    sort = "x desc"
    trans = heatmap_gen(data_file_url, filter, aggregate, encodings, sort)
    print(trans)
