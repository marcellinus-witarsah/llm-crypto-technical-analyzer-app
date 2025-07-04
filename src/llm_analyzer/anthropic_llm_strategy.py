from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from src.llm_analyzer.base_llm_strategy import BaseLLMStrategy
from src.model.analysis import Actions, Analysis

class AnthropicLLMStrategy(BaseLLMStrategy):
    def __init__(self, **kwargs):
        super().__init__()
        self.__llm = ChatAnthropic(**kwargs).with_structured_output(Analysis)
    
    def analyze(self, prompt_template: ChatPromptTemplate, image_base64: str, pair: str, timeframe: str):
        chain = prompt_template | self.__llm
        return chain.invoke(
        {
            "image_base64": image_base64,
            "pair": pair,
            "timeframe": timeframe,
            "actions" : "\n".join(f"{num+1}. {action.value}" for num, action in enumerate(Actions))
        }
    )