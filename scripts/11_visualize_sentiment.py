"""
11_visualize_sentiment.py

Erstellt Visualisierungen auf Basis der Sentiment-Analyse.

Ziele:
- durchschnittliches Sentiment nach Phase darstellen
- Sentiment-Verteilung je Account darstellen

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


def ensure_output_directory() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    phase_df = pd.read_csv(PHASE_INPUT)
    account_df = pd.read_csv(ACCOUNT_INPUT)
    return phase_df, account_df


def plot_sentiment_by_phase(phase_df: pd.DataFrame) -> None:
    plot_df = phase_df.sort_values("phase").copy()

    plt.figure(figsize=(8, 6))
    plt.bar(plot_df["phase"], plot_df["avg_sentiment"])
    plt.title("Durchschnittliches Sentiment nach Phase")
    plt.xlabel("Phase")
    plt.ylabel("Durchschnittlicher Sentiment-Score")
    plt.tight_layout()
    plt.savefig(PHASE_OUTPUT, dpi=300, bbox_inches="tight")
    plt.close()


def plot_sentiment_by_account(account_df: pd.DataFrame) -> None:
    plot_df = account_df.sort_values("avg_sentiment", ascending=False).copy()

    plt.figure(figsize=(12, 7))
    plt.bar(plot_df["account_name"], plot_df["avg_sentiment"])
    plt.title("Durchschnittliches Sentiment je Account")
    plt.xlabel("Account")
    plt.ylabel("Durchschnittlicher Sentiment-Score")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(ACCOUNT_OUTPUT, dpi=300, bbox_inches="tight")
    plt.close()


def main() -> None:
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