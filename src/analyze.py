import argparse
import json
import os

import pandas as pd
import plotly.graph_objects as go
import requests
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from plotly.subplots import make_subplots

from src.utils.common_functions import get_base64_encoded_image
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

    # ===================================================================
    # GENERATE IMAGE
    # ===================================================================
    df_visualize = df[df["date"] >= start_date]

    # Create subplot with 2 rows (OHLC and Volume)
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.1, 0.15, 0.15],
        subplot_titles=(
            "OHLC BTC (USD)",
            "Volume",
            "Stochastic(5, 3, 4)",
            "MACD(12, 26, 9)",
        ),
    )

    # Plot Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df_visualize["date"],
            open=df_visualize["open"],
            high=df_visualize["high"],
            low=df_visualize["low"],
            close=df_visualize["close"],
            name="Price",
            increasing_line_color="green",
            decreasing_line_color="red",
        ),
        row=1,
        col=1,
    )

    # Plot Volume Bar
    fig.add_trace(
        go.Bar(
            x=df_visualize["date"],
            y=df_visualize["volume"],
            name="Volume",
            marker_color="orange",
        ),
        row=2,
        col=1,
    )

    # Plot EMA
    fig.add_trace(
        go.Scatter(
            x=df_visualize["date"],
            y=df_visualize["ema_13"],
            mode="lines",
            name="EMA 13",
            line={"width": 1.5},
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df_visualize["date"],
            y=df_visualize["ema_21"],
            mode="lines",
            name="EMA 21",
            line={"width": 1.5},
        ),
        row=1,
        col=1,
    )

    # Plot Stochastic
    fig.add_trace(
        go.Scatter(
            x=df_visualize["date"],
            y=df_visualize["stochastic_percentage_k"],
            mode="lines",
            name="Stochastic",
            line={"width": 1.5},
        ),
        row=3,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df_visualize["date"],
            y=df_visualize["stochastic_percentage_d"],
            mode="lines",
            name="Stochastic",
            line={"width": 1.5},
        ),
        row=3,
        col=1,
    )

    # Plot MACD
    fig.add_trace(
        go.Scatter(
            x=df_visualize["date"],
            y=df_visualize["macd"],
            mode="lines",
            name="MACD Line",
            line={"width": 1.5},
        ),
        row=4,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df_visualize["date"],
            y=df_visualize["macd_signal_line"],
            mode="lines",
            name="MACD Signal Line",
            line={"width": 1.5},
        ),
        row=4,
        col=1,
    )

    colors = ["green" if val >= 0 else "red" for val in df_visualize["macd_bar"]]
    fig.add_trace(
        go.Bar(
            x=df_visualize["date"],
            y=df_visualize["macd_bar"],
            name="MACD Bar",
            marker_color=colors,
        ),
        row=4,
        col=1,
    )

    # Layout adjustments
    fig.update_layout(
        # title=title,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=1000,
        margin=dict(l=50, r=25, t=50, b=40),
    )

    fig.write_image(f"btc_{timeframe}_ta.png", width=1200, height=800, scale=2)
    image_base64 = get_base64_encoded_image(
        f"btc_{timeframe}_ta.png"
    )  # encode image into base64 format

    # ===================================================================
    # GENERATE PROMPT
    # ===================================================================
    llm = ChatAnthropic(
        model="claude-sonnet-4-20250514",
        max_tokens=20000,
        temperature=0.5,
        timeout=None,
        max_retries=2,
    )

    prompt = ChatPromptTemplate(
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
                        "text": """
                        Your task is to analyze the {timeframe} chart and technical indicators for {ticker} based on the provided image.
                        Perform a technical analysis based on candlestick charts, volume, and technical indicators like {technical_indicators} (shown in image). 
                        Your technical analysis should lead to a recommendation one of the following actions:
                        <actions>
                            1. 'Weak Sell'
                            2. 'Sell'
                            3. 'Strong Sell'
                            4. 'Weak Buy'
                            5. 'Buy'
                            6. 'Strong Buy'
                        </actions>
                        For the prompt output, it should only formatted like in the <structure> tags below. Do not inlcude markdown texts. 
                        <structure>
                            {{
                                \"action\": \"Your recommended action based on the <actions> tags\",    
                                \"reason\": {{
                                    \"ema\": \"EMA analysis\",
                                    \"stochastic\": \"Stochastic oscillator analysis\",
                                    \"macd\": \"MACD analysis\",
                                    \"trend\": \"Overall trend analysis\",
                                    \"pattern\": \"Candlestick pattern analysis\",
                                    \"volume\": \"Volume analysis\"
                                }}
                            }}
                        </structure>
                        """,
                    },
                ],
            },
        ]
    )

    # ===================================================================
    # GIVE TO LLM
    # ===================================================================
    chain = prompt | llm
    response = chain.invoke(
        {
            "image_base64": image_base64,
            "ticker": pair,
            "timeframe": timeframe,
            "technical_indicators": technical_indicators,
        }
    )

    # ===================================================================
    # PROCESS OUTPUT FROM LLM
    # ===================================================================
    message = json.loads(response.content)

    summary = ""
    summary += f"Action: **{message['action'].capitalize()}**\n"
    for num, (indicator, explanation) in enumerate(message["reason"].items()):
        summary += f"{num+1}. **{indicator.upper()}**: {explanation}.\n"
    summary += "*^Disclaimer Alert.*"

    # ===================================================================
    # SEND OUTPUT TO DISCORD CHANNEL
    # ===================================================================
    embed = {
        "title": f"**{pair} {timeframe.capitalize()} Summary**",
        "image": {"url": f"attachment://btc_{timeframe}_ta.png"},
        "description": summary,
        "color": 5814783,
    }
    data = {
        "content": (f"**{pair} {timeframe.capitalize()} Summary**\n\n" f"{summary}")
    }

    files = {"file": (f"btc_{timeframe}_ta.png", open(f"btc_{timeframe}_ta.png", "rb"))}

    # Use `data=` for data when uploading files
    requests.post(os.environ["DISCORD_WEBHOOK_URL"], files=files, data=data)
