import matplotlib.pyplot as plt
import pandas as pd

def donut_series(series: pd.Series, title: str = ""):
    fig, ax = plt.subplots(figsize=(4,4))
    vals = series.fillna(0.0).values
    labels = series.index.tolist()
    wedges, _ = ax.pie(vals, startangle=140)
    centre_circle = plt.Circle((0,0),0.65,fc='black')
    fig.gca().add_artist(centre_circle)
    ax.set_title(title)
    return fig

def heatmap_corr(corr: pd.DataFrame, title: str = "Correlation Heatmap"):
    fig, ax = plt.subplots(figsize=(5,4))
    im = ax.imshow(corr.values, aspect='auto')
    ax.set_xticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=90)
    ax.set_yticks(range(len(corr.index)))
    ax.set_yticklabels(corr.index)
    ax.set_title(title)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    return fig

def line_series(df: pd.DataFrame, title: str = ""):
    fig, ax = plt.subplots(figsize=(6,3))
    for col in df.columns:
        ax.plot(df.index, df[col], label=col)
    ax.legend()
    ax.set_title(title)
    return fig
