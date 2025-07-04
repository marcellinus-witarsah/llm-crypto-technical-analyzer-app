import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

def visualize_chart_and_technical_analysis(df: pd.DataFrame):
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
            "Stochastic(5, 3, 3)",
            "MACD(12, 26, 9)",
        ),
    )

    # Plot Candlestick
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

    # Plot Volume Bar
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

    # Plot EMA
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
    )

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
    )

    # Plot Stochastic
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
    )

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
    )

    # Plot MACD
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
    )

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
    )

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
    )

    # Layout adjustments
    fig.update_layout(
        # title=title,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=1000,
        margin=dict(l=50, r=25, t=50, b=40),
    )
    
    return fig