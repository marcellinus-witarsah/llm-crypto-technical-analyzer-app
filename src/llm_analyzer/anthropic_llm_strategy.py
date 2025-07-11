from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel

from src.llm_analyzer.llm_strategy_interface import LLMStrategyInterface
from src.model.analysis import Actions, Analysis


class AnthropicLLMStrategy(LLMStrategyInterface):
    def __init__(self, **kwargs):
        super().__init__()
        self.__llm = ChatAnthropic(**kwargs).with_structured_output(Analysis)

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
        chain = prompt_template | self.__llm
        return chain.invoke(
            {
                "image_base64": image_base64,
                "pair": pair,
                "timeframe": timeframe,
                "actions": "\n".join(
                    f"{num+1}. {action.value}" for num, action in enumerate(Actions)
                ),
            }
        )
