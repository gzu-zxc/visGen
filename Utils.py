import pandas as pd
from LLM.summarizer import summarize
from LLM.Deepseek_llm import DeepSeekTextGenerator

def filter_data(data_df, filter_massage: str):
    if filter_massage != "none":
        data_df.fillna('null', inplace=True)
        try:
            data_df = data_df.query(filter_massage)
        except Exception as e:
            print(f"数据过滤发生了一个异常: {e}，过滤信息为{filter_massage}")
    return data_df


# 后续优化只跑一次 summary
def gen_fields_type(file_url):
    textgen = DeepSeekTextGenerator()
    summary = summarize(textgen, file_url)
    fields = summary['fields']
    fields_dict = dict()
    for field in fields:
        fields_dict[field['column']] = field["properties"]['visualization_type']
    return fields_dict


if __name__ == '__main__':
    df = pd.DataFrame({
        'Age': [12, 25, 36, 48],
        'COMMISSION_PCT': ['null', '0.1', '0.2', 'null'],
        'DEPARTMENT_ID': [10, 20, 30, 40]
    })
    print(filter_data(df, "Age > 50 or Age < 46"))
