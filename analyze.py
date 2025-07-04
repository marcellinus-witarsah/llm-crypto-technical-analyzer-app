import argparse
import os

import pandas as pd
import requests
from dotenv import load_dotenv
from src.llm_analyzer.llm_analyzer_interface import LLMAnalyzerInterface
from src.llm_analyzer.anthropic_llm_strategy import AnthropicLLMStrategy
from langchain_core.prompts import ChatPromptTemplate

from src.utils.common_functions import get_base64_encoded_image
from src.utils.visualization import visualize_chart_and_technical_analysis
from src.utils.timescaledb_ops import TimescaleDBOps

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pair",
        type=str,
        required=True,
        help="Cryptocurrency asset pair that wanted to be analyzed.",
    )

    parser.add_argument(
        "--timeframe",
        type=str,
        required=True,
        help="Timeframe of the cryptocurrency asset.",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="The start date to process the data in YYYY-MM-DD format",
    )

    # ===================================================================
    # GET DATA
    # ===================================================================
    args = parser.parse_args()
    pair = args.pair
    timeframe = args.timeframe
    start_date = args.start_date
    technical_indicators = "EMA, Stochastic, MACD"
    
    tsdb_ops = TimescaleDBOps()
    columns, data = tsdb_ops.read_data(f"gold.ohlc_ta_{timeframe}")
    df = pd.DataFrame(data=data, columns=columns)

    # ===================================================================
    # FILTER DATA
    # ===================================================================
    df = df[df["pair"] == pair]
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= start_date]

    # ===================================================================
    # GENERATE IMAGE
    # ===================================================================
    fig = visualize_chart_and_technical_analysis(df)
    fig.write_image(f"btc_{timeframe}_ta.png", width=1200, height=800, scale=2)
    image_base64 = get_base64_encoded_image(
        f"btc_{timeframe}_ta.png"
    )  # encode image into base64 format

    # ===================================================================
    # GENERATE PROMPT
    # ===================================================================
    llm = LLMAnalyzerInterface(
        AnthropicLLMStrategy(
            model="claude-sonnet-4-20250514",
            max_tokens=20000,
            temperature=0,
            timeout=None,
            max_retries=2
        )
    )

    chat_prompt_template = ChatPromptTemplate(
        [
            {
                "role": "system", 
                "content": [
                    {
                        "type": "text",
                        "text": "You are a professional cryptocurrency trader at a top proprietary trading firm, specializing in technical analysis."
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source_type": "base64",
                        "mime_type": "image/png",
                        "data": "{image_base64}",
                    },
                    {
                        "type": "text",
                        "text": (
                            "Your task is to analyze the {timeframe} chart and technical indicators for {pair} based on the provided image. "
                            "Perform a technical analysis based on candlestick charts, and technical indicators shown in image. "
                            "Your technical analysis should lead to a recommendation one of the following actions:\n"
                            "{actions}"
                        )
                    }
                ]
            }
        ],
        input_variables = ["timeframe", "pair", "actions", "image_base64"],
    )
    # ===================================================================
    # GIVE TO LLM
    # ===================================================================
    response = llm.analyze(
        prompt_template = chat_prompt_template,
        image_base64 = image_base64,
        pair = pair,
        timeframe = timeframe,
    )

    # ===================================================================
    # PROCESS OUTPUT FROM LLM
    # ===================================================================
    summary = ""
    summary += f"Action: **{response.action.value}**\n"
    for num, (indicator, explanation) in enumerate(response.reasons.items()):
        summary += f"**{indicator}**\n{explanation}.\n\n"
    summary += "*^Disclaimer Alert.*"

    # ===================================================================
    # SEND OUTPUT TO DISCORD CHANNEL
    # ===================================================================
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    embed = {
        "title": f"**Bitcoin (BTC-USD) {timeframe.capitalize()} Summary**",
        "image": {"url": f"attachment://btc_{timeframe}_ta.png"},
        "description": summary,
        "color": 5814783  # optional: light blue
    }
    data = {
        "content": (
            f"**Bitcoin (BTC-USD) {timeframe.capitalize()} Summary**\n\n"
            f"{summary}"
        )
    }


    files = {
        "file": (
            "btc_monthly_ta.png", 
            open("C:/Users/USER/projects/ai-agent-crypto-analyzer/btc_daily_ta.png", "rb")
        )
    }

    # Use `data=` for data when uploading files
    requests.post(webhook_url, files=files, data=data)
