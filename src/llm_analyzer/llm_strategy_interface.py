from abc import ABC, abstractmethod

from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel


class LLMStrategyInterface(ABC):
    @abstractmethod
    def analyze(
        self,
        prompt_template: ChatPromptTemplate,
        image_base64: str,
        pair: str,
        timeframe: str,
    ) -> dict | BaseModel:
        pass
