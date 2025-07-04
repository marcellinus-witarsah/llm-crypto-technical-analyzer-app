from abc import ABC, abstractmethod
from langchain_core.prompts import ChatPromptTemplate
from src.schemas.analysis_response import Analysis

class BaseLLMStrategy(ABC):
    @abstractmethod
    def analyze(self, prompt_template: ChatPromptTemplate, image_base64: str, pair: str, timeframe: str) -> Analysis:
        pass