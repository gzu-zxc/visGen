from typing import Union, List
from openai import  OpenAI
from text_generator import TextGenerator


class DeepSeekTextGenerator(TextGenerator):
    def __init__(
            self
    ):
        super().__init__()
        self.client = None
        self.model_name = "deepseek-coder"

        client_args = {
            "api_key": "",
            "base_url": "https://api.deepseek.com/v1"
        }
        self.client = OpenAI(**client_args)

    def generate(
            self,
            messages: Union[List[dict], str],
    ):

        return self.client.chat.completions.create(model=self.model_name, messages=messages, stream=False,
                                                   temperature=0.0).choices[0].message.content

    def generate_nl(
            self,
            messages: Union[List[dict], str],

    ):
        self.model_name = "deepseek-chat"

        return self.client.chat.completions.create(model=self.model_name, messages=messages, stream=False,
                                                   temperature=0.0).choices[0].message.content
