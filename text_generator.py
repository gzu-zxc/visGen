import os
from typing import Union, List, Dict
from abc import ABC, abstractmethod


class TextGenerator(ABC):

    def __init__(self, llm_type: str = "openai"):
        self.llm_type = llm_type

    @abstractmethod
    def generate(
            self, messages: Union[List[Dict], str]):
        pass
