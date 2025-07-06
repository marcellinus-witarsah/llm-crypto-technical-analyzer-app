from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel

from src.llm_analyzer.llm_strategy_interface import LLMStrategyInterface


class LLMAnalyzer:
    # Define constructors

    def __init__(self, llm_strategy: LLMStrategyInterface):
        self.__llm_strategy = llm_strategy

    # Define setters and getters
    @property
    def llm_strategy(self):
        return self.__llm_strategy

    @llm_strategy.setter
    def set_llm_strategy(self, llm_strategy: LLMStrategyInterface):
        self.__llm_strategy = llm_strategy

    # Define abstract method for analysis
    def analyze(
        self,
        prompt_template: ChatPromptTemplate,
        image_base64: str,
        pair: str,
        timeframe: str,
    ) -> dict | BaseModel:
        """Perform technical analysis

        Args:
            prompt_template (ChatPromptTemplate): prompt template that will be given to LLM.
            image_base64 (str): chart and technical indicators image in base64 format.
            pair (str): pair that will be analyzed.
            timeframe (str): timeframe of the chart.

        Returns:
            dict | BaseModel: output from LLM.
        """
        return self.llm_strategy.analyze(
            prompt_template=prompt_template,
            image_base64=image_base64,
            pair=pair,
            timeframe=timeframe,
        )
