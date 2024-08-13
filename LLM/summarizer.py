import logging
from LLM.datacleaning import read_dataframe, get_column_properties
import json
from LLM.text_generator import TextGenerator

logger = logging.getLogger("zxc")

system_prompt = """
You are an experienced data analyst that can annotate datasets. Your instructions are as follows:
i) ALWAYS generate the name of the dataset and the dataset_description
ii) ALWAYS generate a field description.
iii) ALWAYS generate a visualization_type (a single word) for each field given its values e.g. quantitative, nominal, temporal, ordinal, GeoJSON
iiii) Set the ID's visualization_type to nominal
"""


def enrich(base_summary: dict, text_gen: TextGenerator) -> dict:
    """Enrich the data summary with descriptions"""
    logger.info(f"Enriching the data summary with descriptions")

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"""
    Annotate the dictionary below. Only return a JSON object.
    {base_summary}
    """},
    ]

    # if isinstance(text_gen, GeMiniTextGenerator):
    #     response = text_gen.generate(messages=messages[0]["content"] + messages[1]["content"])
    #     cleaned_json_string = response
    # else:
    response = text_gen.generate(messages=messages)
    cleaned_json_string = response.replace("```json", "").replace("```", "")
    enriched_summary = json.loads(cleaned_json_string)

    return enriched_summary


def summarize(text_gen: TextGenerator, file_location: str, n_samples: int = 5,
              summary_method: str = "llm", encoding: str = 'utf-8') -> dict:
    """Summarize data from a pandas DataFrame or a file location"""

    file_name = file_location.split("/")[-1]
    # modified to include encoding
    data = read_dataframe(file_location, encoding=encoding)
    data_properties = get_column_properties(data, n_samples)

    # default single stage summary construction
    base_summary = {
        "name": file_name,
        "file_name": file_name,
        "dataset_description": "",
        "fields": data_properties,
    }
    data_summary = base_summary

    if summary_method == "llm":
        # two stage summarization with llm enrichment

        data_summary = enrich(
            base_summary,
            text_gen=text_gen)


    elif summary_method == "columns":
        # no enrichment, only column names
        # 不需要使用LLM增强，仅仅需要字段名称
        data_summary = {
            "name": file_name,
            "file_name": file_name,
            "dataset_description": ""
        }

    data_summary["field_names"] = data.columns.tolist()
    data_summary["file_name"] = file_name

    return data_summary
