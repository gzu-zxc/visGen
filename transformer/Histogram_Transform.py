from Data_Transform import Data_Transform
import pandas as pd
from LLM.summarizer import summarize
from LLM.Deepseek_llm import DeepSeekTextGenerator
from Utils import filter_data, gen_fields_type,df_for_list


class Histogram_Transform(Data_Transform):
    def __init__(self, data_file_url: str, filter: str, aggregate: str, encoding: str):
        super().__init__(data_file_url, filter, aggregate, encoding)
        self.mark = "histogram"
        self.group = self.judge_group()

    def judge_group(self):
        if len(self.encoding['x'].split()) > 1:
            return {"groupby": self.encoding["y"], "grouped": self.encoding["x"].split()[1]}
        else:
            return {"groupby": self.encoding["x"], "grouped": self.encoding["y"].split()[1]}

    def transform(self):
        # histogram 只输出一列数据
        df = pd.read_csv(self.file_url)
        # 对数据进行过滤
        df = filter_data(df, self.filter)
        merged_df = df[self.group['groupby']].reset_index()
        merged_df.fillna(0, inplace=True)
        return df_for_list(merged_df)
