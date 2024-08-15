import json
import sys
from transformer import Bar_Transform as BT
from LLM.Deepseek_llm import DeepSeekTextGenerator
from LLM.summarizer import summarize
#类型转换
def get_axis_type(axis_key, result_dict):
    default_type_x='category'
    default_type_y='value'
    default_type=""
    if axis_key=='x':
        default_type=default_type_x
    else :
        default_type=default_type_y
    axis_type = result_dict.get(axis_key, default_type)
    if axis_type == 'category':
        return 'category'
    elif axis_type == 'number':
        return 'value'
    elif axis_type == 'string':
        return 'category'  #
    elif axis_type == 'temporal':
        return 'time'
    else:
       return default_type
def Bar_to_Echarts(data_file_url, aggregate, encodings, filter, sort):
    trans = BT.Bar_Transform(data_file_url, filter, aggregate, encodings)
    trans_list = trans.transform()
    textgen = DeepSeekTextGenerator()
    summary = summarize(textgen, data_file_url)
    encodings_str = encodings
    pairs = encodings_str.split(',')
    encode = {}
    for pair in pairs:
        key, value = pair.split('=')
        key = key.strip()
        value = value.strip()
        encode[key] = value
    trans_list[0][1]=aggregate
    dimensions = trans_list[0]
    result = []
    if encode.get('color') != "none":
        name = trans_list[0][0]
        echarts_config = ""
        echarts_config = {
            "dataset": [
                {
                    "id": "initial",
                    "dimensions": trans_list[0],
                    "source": trans_list[1:]}
            ],
            "tooltip":{"show":"true"},
            "legend": {
                "show": "true",
                "data": trans_list[0][1:]},
            "yAxis": {
                "type": "value"
            },
            "xAxis": {
                "type": "category"
            },
            "series": []
        }
        echarts_config["series"] = []
        for elenment in trans_list[0][1:]:
            series_item = {
                "type": 'bar',
                "datasetId": "initial",
                "encode": {
                    "x": name,
                    "y": elenment
                },
                "stack": 'total_stack'
            }
            echarts_config["series"].append(series_item)
        with open('echarts_config.json', 'w', encoding='utf-8') as f:
            json.dump(echarts_config, f, ensure_ascii=False, indent=4)
            print("ECharts配置文件已生成：echarts_config.json")
        sys.exit()
    for dimension in dimensions:
        for field in summary['fields']:
            if field['column'] == dimension:
                result.append({"name": dimension, "type": field['properties']['dtype']})
                break  # 找到匹配的列后跳出内层循环
    dataset_dimension=result
    extracted_dtypes = result
    for item in dataset_dimension:
        if 'type' in item:
            item['type'] = 'string'
    if (aggregate):
        dataset_dimension.append({"name": aggregate, "type": "value"})
    if (encode.get('color')!='none'):  # 存在颜色通道
        dataset_dimension.append({"name": encode.get('color'), "type": "value"})
    result_dict = {dim["name"]: dim["type"] for dim in dataset_dimension}
    source = trans_list[1:]
    echarts_config = {
            "dataset": [{
                "id": "initial",
                "dimensions": dataset_dimension,
                "source": source,
            }],
            "title": {"text": "ECharts 示例"},
            "tooltip": {
                "show": "true"
            },
            "legend": {
                "show": "true"
            },
            "xAxis":{
                "name":encode.get('x','none'),
                "type":get_axis_type(encode.get('x', ''), result_dict)
            },
            "yAxis": {
                "name":encode.get('y','none'),
                "type": get_axis_type(encode.get('y', ''), result_dict)
            },
            "series": [{
                "datasetId": "initial",
                "type": 'bar',
                "encode": encode
            }]
        }
    if sort!='none':
        sort_dimension, sort_order = sort.split()
        sort_dimension = encode.get(sort_dimension, 'none')
        print(sort_dimension)
        echarts_config["dataset"].append({
                "id": "sorted_data",
                "fromDatasetId": "initial",
                "transform": {
                    "type": "sort",
                    "config": {
                        "dimension": sort_dimension,
                        "order": sort_order,
                    }
                }
            })
        echarts_config["series"][0]["datasetId"] = "sorted_data"
    with open('echarts_config.json', 'w', encoding='utf-8') as f:
        json.dump(echarts_config, f, ensure_ascii=False, indent=4)
        print("ECharts配置文件已生成：echarts_config.json")
#调用函数
if __name__ == '__main__':
    data_file_url = r"../spider_csv/activity_1_Faculty.csv"
    aggregate = "count Sex"
    encodings = "x=Sex,y=count Sex,color=none,size=none"
    filter = "none"
    sort = "none"
    Bar_to_Echarts(data_file_url, aggregate, encodings, filter, sort)