from transformer.Pie_Transform import Pie_Transform
import json
from Utils import gen_fields_type

template = """
{
  "legend": {},
  "tooltip": {},
  "dataset": [{
    "id": "init",
    "dimensions": ["product", "2012", "2013", "2014", "2015", "2016", "2017"],
    "source": [
      ["Milk Tea", 86.5, 92.1, 85.7, 83.1, 73.4, 55.1],
      ["Matcha Latte", 41.1, 30.4, 65.1, 53.3, 83.8, 98.7],
      ["Cheese Cocoa", 24.1, 67.2, 79.5, 86.4, 65.2, 82.5],
      ["Walnut Brownie", 55.2, 67.1, 69.2, 72.4, 53.9, 39.1]
    ]
  }],
  "series": [
    {
      "datasetIndex": 0,
      "type": "pie",
      "encode": {
        "itemName": "product",
        "value": "2012"
      }
    }
  ]
}
"""


def pie_gen(file_url, filter, aggregate, encoding, sort):
    fields_type = gen_fields_type(file_url)
    trans = Pie_Transform(file_url, filter, aggregate, encoding)
    data_list = trans.transform()
    template_dict = json.loads(template)
    fields_name = data_list[0]
    data_list = data_list[1:]
    dimensions_list = []

    # -----------------------------------------设置 dataset---------------------------------------
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
        # Pie 没有排序
        # -----------------------------------------设置 series---------------------------------------
        template_dict['series'][0]['encode']['itemName'] = fields_name[0]
        template_dict['series'][0]['encode']['value'] = fields_name[1]

    return template_dict


if __name__ == '__main__':
    data_file_url = r"../spider_csv/college_1_course.csv"
    aggregate = "sum CRS_CREDIT"
    encodings = "x=DEPT_CODE,y=sum CRS_CREDIT,color=none,size=none"
    filter = "none"
    sort = "none"
    trans = pie_gen(data_file_url, filter, aggregate, encodings, sort)
    print(trans)
