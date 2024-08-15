import pandas as pd
from LLM.summarizer import summarize
from LLM.Deepseek_llm import DeepSeekTextGenerator


def filter_data(data_df, filter_massage):
    """
    进行数据过滤

    :param data_df: 需要进行数据过滤的dataframe
    :param filter_massage: 包括过滤信息的字符串
    :return: 返回过滤操作后的 dataframe
    """
    if filter_massage != "none":
        data_df.fillna('null', inplace=True)
        try:
            data_df = data_df.query(filter_massage)
        except Exception as e:
            print(f"数据过滤发生了一个异常: {e}，过滤信息为{filter_massage}")
    return data_df


# 后续优化只跑一次 summary
def gen_fields_type(file_url):
    """
    生成数据文件的各个数据字段的可视化类型
    :param file_url: 需要获取的数据文件 URL
    :return: 返回一个包含数据类型的 dict，例如：{"field_name":"field_type"}
    """
    textgen = DeepSeekTextGenerator()
    summary = summarize(textgen, file_url)
    fields = summary['fields']
    fields_dict = dict()
    for field in fields:
        fields_dict[field['column']] = field["properties"]['visualization_type']
    return fields_dict


def df_for_list(df):
    """
    将经过数据转换的dataframe，转换为 List[List] 形式
    :param df: 需要转换的 dataframe
    :return: 例如：[['date_address_to', 'house', 'apartment'], ['2018-02', 1, 1], ['2018-03', 12, 6]]
    """
    list_of_columns = df.columns.tolist()
    list_of_rows = df.values.tolist()
    list_of_rows.insert(0, list_of_columns)
    return list_of_rows
