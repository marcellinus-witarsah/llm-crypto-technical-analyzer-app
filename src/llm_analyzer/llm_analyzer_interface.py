from langchain_core.prompts import ChatPromptTemplate
from src.llm_analyzer.base_llm_strategy import BaseLLMStrategy
from src.model.analysis import Analysis

class LLMAnalyzerInterface():
    # Define constructors
    
    def __init__(self, llm_strategy: BaseLLMStrategy):
        self.__llm_strategy = llm_strategy
    
    # Define setters and getters
    @property
    def llm_strategy(self):
        return self.__llm_strategy
    
    @llm_strategy.setter
    def set_llm_strategy(self, llm_strategy: BaseLLMStrategy):
        self.__llm_strategy = llm_strategy
    
    # Define abstract method for analysis
    def analyze(self, prompt_template: ChatPromptTemplate, image_base64: str, pair: str, timeframe: str) -> Analysis:
        return self.llm_strategy.analyze(
            prompt_template=prompt_template,    
            image_base64=image_base64,
            pair=pair, 
            timeframe=timeframe
        )