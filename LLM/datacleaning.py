import warnings
import pandas as pd
import logging
import re

logger = logging.getLogger("zxc")


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean all column names in the given DataFrame.
    清除给定 DataFrame 中的所有列名。

    :param df: The DataFrame with possibly dirty column names.
    :return: A copy of the DataFrame with clean column names.
    """
    cleaned_df = df.copy()
    cleaned_df.columns = [clean_column_name(col) for col in cleaned_df.columns]
    return cleaned_df


def clean_column_name(col_name: str) -> str:
    """
    Clean a single column name by replacing special characters and spaces with underscores.
    用下划线替换特殊字符和空格，清理单列名称。

    :param col_name: The name of the column to be cleaned.
    :return: A sanitized string valid as a column name.
    """
    return re.sub(r'[^0-9a-zA-Z_]', '_', col_name)


def read_dataframe(file_location: str, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Read a dataframe from a given file location and clean its column names.
    It also samples down to 4500 rows if the data exceeds that limit.
    从指定文件位置读取数据帧，并清除其列名。
    如果数据超过 4500 行的限制，它还会向下采样至 4500 行。

    :param file_location: The path to the file containing the data.
    :param encoding: Encoding to use for the file reading.
    :return: A cleaned DataFrame.
    """
    file_extension = file_location.split('.')[-1]

    read_funcs = {
        'json': lambda: pd.read_json(file_location, orient='records', encoding=encoding),
        'csv': lambda: pd.read_csv(file_location, encoding=encoding),
        'xls': lambda: pd.read_excel(file_location),
        'xlsx': lambda: pd.read_excel(file_location),
        'parquet': pd.read_parquet,
        'feather': pd.read_feather,
        'tsv': lambda: pd.read_csv(file_location, sep="\t", encoding=encoding)
    }

    if file_extension not in read_funcs:
        raise ValueError('Unsupported file type')

    try:
        df = read_funcs[file_extension]()
    except Exception as e:
        logger.error(f"Failed to read file: {file_location}. Error: {e}")
        raise

    # Clean column names
    cleaned_df = df

    # Sample down to 4500 rows if necessary
    if len(cleaned_df) > 4500:
        logger.info(
            "Dataframe has more than 4500 rows. We will sample 4500 rows.")
        cleaned_df = cleaned_df.sample(4500)

    if cleaned_df.columns.tolist() != df.columns.tolist():
        write_funcs = {
            'spider_csv': lambda: cleaned_df.to_csv(file_location, index=False, encoding=encoding),
            'xls': lambda: cleaned_df.to_excel(file_location, index=False),
            'xlsx': lambda: cleaned_df.to_excel(file_location, index=False),
            'parquet': lambda: cleaned_df.to_parquet(file_location, index=False),
            'feather': lambda: cleaned_df.to_feather(file_location, index=False),
            'json': lambda: cleaned_df.to_json(file_location, orient='records', index=False, default_handler=str),
            'tsv': lambda: cleaned_df.to_csv(file_location, index=False, sep='\t', encoding=encoding)
        }

        if file_extension not in write_funcs:
            raise ValueError('Unsupported file type')

        try:
            write_funcs[file_extension]()
        except Exception as e:
            logger.error(f"Failed to write file: {file_location}. Error: {e}")
            raise

    return cleaned_df


def check_type(dtype: str, value):
    """Cast value to right type to ensure it is JSON serializable"""
    # 将值转换为正确的类型，以确保其可被 JSON 序列化
    if "float" in str(dtype):
        return float(value)
    elif "int" in str(dtype):
        return int(value)
    else:
        return value


def get_column_properties(df: pd.DataFrame, n_samples: int = 5) -> list[dict]:
    """Get properties of each column in a pandas DataFrame"""
    # 获取 pandas DataFrame 中每一列的属性

    properties_list = []
    for column in df.columns:
        dtype = df[column].dtype
        properties = {}
        if dtype in [int, float, complex]:
            properties["dtype"] = "number"
            properties["std"] = check_type(dtype, df[column].std())
            properties["min"] = check_type(dtype, df[column].min())
            properties["max"] = check_type(dtype, df[column].max())

        elif dtype == bool:
            properties["dtype"] = "boolean"
        elif dtype == object:
            # Check if the string column can be cast to a valid datetime
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    pd.to_datetime(df[column], errors='raise')
                    properties["dtype"] = "date"
            except ValueError:
                # Check if the string column has a limited number of values
                if df[column].nunique() / len(df[column]) < 0.5:
                    properties["dtype"] = "category"
                else:
                    properties["dtype"] = "string"
        elif pd.api.types.is_categorical_dtype(df[column]):
            properties["dtype"] = "category"
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            properties["dtype"] = "date"
        else:
            properties["dtype"] = str(dtype)

        # add min max if dtype is date
        if properties["dtype"] == "date":
            try:
                properties["min"] = df[column].min()
                properties["max"] = df[column].max()
            except TypeError:
                cast_date_col = pd.to_datetime(df[column], errors='coerce')
                properties["min"] = cast_date_col.min()
                properties["max"] = cast_date_col.max()
        # Add additional properties to the output dictionary
        nunique = df[column].nunique()
        if "samples" not in properties:
            non_null_values = df[column][df[column].notnull()].unique()
            n_samples = min(n_samples, len(non_null_values))
            samples = pd.Series(non_null_values).sample(
                n_samples, random_state=42).tolist()
            properties["samples"] = samples
        properties["num_unique_values"] = nunique
        properties["visualization_type"] = ""
        properties["description"] = ""
        properties_list.append(
            {"column": column, "properties": properties})

    return properties_list


if __name__ == '__main__':
    df = read_dataframe('data/airports.csv')
    print(get_column_properties(df, 5))
