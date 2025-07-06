import argparse
import os

import pandas as pd
import plotly.graph_objects as go
import requests
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from plotly.subplots import make_subplots

from llm_analyzer.llm_analyzer import LLMAnalyzer
from src.llm_analyzer.anthropic_llm_strategy import AnthropicLLMStrategy
from src.utils.common_functions import get_base64_encoded_image
from timescaledb_ops import TimescaleDBOps

if __name__ == "__main__":
    # =========================================================================
    # Create parser for processing CLI arguments
    # =========================================================================
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
    args = parser.parse_args()
    pair = args.pair
    timeframe = args.timeframe
    start_date = args.start_date

    # =========================================================================
    # Load environment variables
    # =========================================================================
    load_dotenv()

    # =========================================================================
    # Load and filter data from Database
    # =========================================================================
    # Load data from Database
    tsdb_ops = TimescaleDBOps()
    columns, data = tsdb_ops.read_data(f"gold.ohlc_ta_{timeframe}")
    df = pd.DataFrame(data=data, columns=columns)

    # Filter data from Database
    df = df[df["pair"] == pair]
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= start_date]

    # =========================================================================
    # Create an image containing charts and technical indicators
    # =========================================================================
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.1, 0.15, 0.15],
        subplot_titles=(
            pair,
            "Volume",
            "Stochastic(5, 3, 3)",
            "MACD(12, 26, 9)",
        ),
    )

    # Create candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Price",
            increasing_line_color="green",
            decreasing_line_color="red",
        ),
        row=1,
        col=1,
    )

    # Create volume bar chart
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=df["volume"],
            name="Volume",
            marker_color="orange",
        ),
        row=2,
        col=1,
    )

    # Create EMA chart
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["ema_13"],
            mode="lines",
            name="EMA 13",
            line={"width": 1.5},
        ),
        row=1,
        col=1,
    )  # 13 EMA line

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["ema_21"],
            mode="lines",
            name="EMA 21",
            line={"width": 1.5},
        ),
        row=1,
        col=1,
    )  # 21 EMA line

    # Create Stochastic Chart
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["stochastic_percentage_k"],
            mode="lines",
            name="Stochastic %K",
            line={"width": 1.5},
        ),
        row=3,
        col=1,
    )  # Stochastic %k line

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["stochastic_percentage_d"],
            mode="lines",
            name="Stochastic %D",
            line={"width": 1.5},
        ),
        row=3,
        col=1,
    )  # Stochastic %d line

    # Create MACD Chart
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["macd"],
            mode="lines",
            name="MACD Line",
            line={"width": 1.5},
        ),
        row=4,
        col=1,
    )  # Create MACD Line

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["macd_signal_line"],
            mode="lines",
            name="MACD Signal Line",
            line={"width": 1.5},
        ),
        row=4,
        col=1,
    )  # Create MACD Signal Line

    colors = ["green" if val >= 0 else "red" for val in df["macd_bar"]]
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=df["macd_bar"],
            name="MACD Bar",
            marker_color=colors,
        ),
        row=4,
        col=1,
    )  # Create MACD Bar

    # Layout adjustments
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=1000,
        margin=dict(l=50, r=25, t=50, b=40),
    )

    # Save the image
    fig.write_image(f"btc_{timeframe}_ta.png", width=1200, height=800, scale=2)

    # Encode image into base64 format
    image_base64 = get_base64_encoded_image(f"btc_{timeframe}_ta.png")

    # =========================================================================
    # Prepare LLM Analyzer and Prompt
    # =========================================================================
    llm = LLMAnalyzer(
        AnthropicLLMStrategy(
            model="claude-sonnet-4-20250514",
            max_tokens=20000,
            temperature=0,
            timeout=None,
            max_retries=2,
        )
    )

    chat_prompt_template = ChatPromptTemplate(
        [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "You are a professional cryptocurrency trader at a top proprietary trading firm, specializing in technical analysis.",
                    }
                ],
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
                        ),
                    },
                ],
            },
        ],
        input_variables=["timeframe", "pair", "actions", "image_base64"],
    )

    # =========================================================================
    # Give Prompt to LLM
    # =========================================================================
    response = llm.analyze(
        prompt_template=chat_prompt_template,
        image_base64=image_base64,
        pair=pair,
        timeframe=timeframe,
    )

    # =========================================================================
    # Process Output from LLM
    # =========================================================================
    summary = ""
    summary += f"Action: **{response.action.value}**\n"
    for num, (indicator, explanation) in enumerate(response.reasons.items()):
        summary += f"**{indicator}**\n{explanation}.\n\n"
    summary += "*^Disclaimer Alert.*"

    # =========================================================================
    # Send the summary to Discord Channel via WebHook
    # =========================================================================
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    embed = {
        "title": f"**Bitcoin (BTC-USD) {timeframe.capitalize()} Summary**",
        "image": {"url": f"attachment://btc_{timeframe}_ta.png"},
        "description": summary,
        "color": 5814783,  # optional: light blue
    }
    data = {
        "content": (
            f"**Bitcoin (BTC-USD) {timeframe.capitalize()} Summary**\n\n" f"{summary}"
        )
    }

    files = {
        "file": (
            "btc_monthly_ta.png",
            open(
                "C:/Users/USER/projects/ai-agent-crypto-analyzer/btc_daily_ta.png", "rb"
            ),
        )
    }

    # Use `data=` for data when uploading files
    requests.post(webhook_url, files=files, data=data)

    # =========================================================================
    # Remove unwanted data
    # =========================================================================
    os.remove(f"btc_{timeframe}_ta.png")
