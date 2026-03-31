"""
11_visualize_sentiment.py

Erstellt Visualisierungen auf Basis der Sentiment-Analyse.

Ziele:
- durchschnittliches Sentiment nach Phase darstellen
- durchschnittliches Sentiment je Account darstellen

Eingabedateien:
- data/processed/sentiment_by_phase.csv
- data/processed/sentiment_by_account.csv

Ausgabedateien:
- figures/sentiment_by_phase.png
- figures/sentiment_by_account.png
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


INPUT_DIR = Path("data") / "processed"
OUTPUT_DIR = Path("figures")

PHASE_INPUT = INPUT_DIR / "sentiment_by_phase.csv"
ACCOUNT_INPUT = INPUT_DIR / "sentiment_by_account.csv"

PHASE_OUTPUT = OUTPUT_DIR / "sentiment_by_phase.png"
ACCOUNT_OUTPUT = OUTPUT_DIR / "sentiment_by_account.png"

PHASE_ORDER = ["Phase A", "Phase B"]

FIGSIZE_PHASE = (8, 6)
FIGSIZE_ACCOUNT = (12, 7)
DPI = 300

TITLE_SIZE = 18
LABEL_SIZE = 13
TICK_SIZE = 11


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Zielordner für Grafiken existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lädt die Sentiment-Daten.
    """
    phase_df = pd.read_csv(PHASE_INPUT)
    account_df = pd.read_csv(ACCOUNT_INPUT)
    return phase_df, account_df


def add_bar_labels(ax: plt.Axes, decimals: int = 5) -> None:
    """
    Schreibt numerische Werte oberhalb der Balken.
    """
    for container in ax.containers:
        labels = []
        for bar in container:
            height = bar.get_height()
            if pd.isna(height):
                labels.append("")
            else:
                labels.append(f"{height:.{decimals}f}")
        ax.bar_label(container, labels=labels, padding=3, fontsize=10)


def plot_sentiment_by_phase(phase_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik des durchschnittlichen Sentiments nach Phase.
    """
    plot_df = phase_df.copy()

    if "phase" in plot_df.columns:
        plot_df["phase"] = pd.Categorical(
            plot_df["phase"],
            categories=PHASE_ORDER,
            ordered=True
        )
        plot_df = plot_df.sort_values("phase")

    fig, ax = plt.subplots(figsize=FIGSIZE_PHASE)
    bars = ax.bar(plot_df["phase"].astype(str), plot_df["avg_sentiment"])

    ax.set_title("Durchschnittliches Sentiment nach Phase", fontsize=TITLE_SIZE, pad=12)
    ax.set_xlabel("Phase", fontsize=LABEL_SIZE)
    ax.set_ylabel("Durchschnittlicher Sentiment-Score", fontsize=LABEL_SIZE)
    ax.tick_params(axis="x", labelsize=TICK_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_SIZE)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    ax.bar_label(
        bars,
        labels=[f"{value:.5f}" for value in plot_df["avg_sentiment"]],
        padding=3,
        fontsize=10
    )

    ymin = min(plot_df["avg_sentiment"].min(), 0)
    ymax = plot_df["avg_sentiment"].max()
    y_range = ymax - ymin
    padding = max(y_range * 0.15, 0.0005)
    ax.set_ylim(ymin - padding * 0.2, ymax + padding)

    plt.tight_layout()
    plt.savefig(PHASE_OUTPUT, dpi=DPI, bbox_inches="tight")
    plt.close()


def plot_sentiment_by_account(account_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik des durchschnittlichen Sentiments je Account.
    """
    plot_df = account_df.sort_values("avg_sentiment", ascending=False).copy()

    fig, ax = plt.subplots(figsize=FIGSIZE_ACCOUNT)
    bars = ax.bar(plot_df["account_name"], plot_df["avg_sentiment"])

    ax.set_title("Durchschnittliches Sentiment je Account", fontsize=TITLE_SIZE, pad=12)
    ax.set_xlabel("Account", fontsize=LABEL_SIZE)
    ax.set_ylabel("Durchschnittlicher Sentiment-Score", fontsize=LABEL_SIZE)
    ax.tick_params(axis="x", rotation=45, labelsize=TICK_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_SIZE)
    plt.xticks(ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    ax.bar_label(
        bars,
        labels=[f"{value:.5f}" for value in plot_df["avg_sentiment"]],
        padding=3,
        fontsize=9
    )

    ymin = min(plot_df["avg_sentiment"].min(), 0)
    ymax = plot_df["avg_sentiment"].max()
    y_range = ymax - ymin
    padding = max(y_range * 0.12, 0.0005)
    ax.set_ylim(ymin - padding * 0.2, ymax + padding)

    plt.tight_layout()
    plt.savefig(ACCOUNT_OUTPUT, dpi=DPI, bbox_inches="tight")
    plt.close()


def main() -> None:
    """
    Führt die Visualisierung der Sentiment-Daten vollständig aus.
    """
    ensure_output_directory()

    print("Lade Sentiment-Daten ...")
    phase_df, account_df = load_data()

    print("Erstelle Grafik: Sentiment nach Phase ...")
    plot_sentiment_by_phase(phase_df)

    print("Erstelle Grafik: Sentiment je Account ...")
    plot_sentiment_by_account(account_df)

    print("Visualisierung abgeschlossen.")
    print(f"- {PHASE_OUTPUT}")
    print(f"- {ACCOUNT_OUTPUT}")


if __name__ == "__main__":
    main()