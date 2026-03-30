"""
10_sentiment_analysis.py

Führt eine einfache Sentiment-Analyse auf Basis der vorbereiteten
Posts in `posts_prepared` durch.

Ziele:
- Sentiment pro Post berechnen
- Sentiment-Klassen zuweisen
- Ergebnisse je Phase und je Account aggregieren
- Analyseergebnisse als CSV-Dateien speichern

Hinweis:
Die erste Version verwendet TextBlob als schlanke Baseline-Lösung.
Für deutschsprachige politische Texte ist das methodisch nicht perfekt,
aber als transparenter und reproduzierbarer MVP geeignet.
"""

from pathlib import Path
import sqlite3
import pandas as pd
from textblob import TextBlob


DATABASE_PATH = Path("database") / "election_posts.db"
OUTPUT_DIR = Path("data") / "processed"

POST_LEVEL_OUTPUT = OUTPUT_DIR / "sentiment_post_level.csv"
PHASE_OUTPUT = OUTPUT_DIR / "sentiment_by_phase.csv"
ACCOUNT_OUTPUT = OUTPUT_DIR / "sentiment_by_account.csv"


SELECT_PREPARED_POSTS_SQL = """
SELECT
    post_id,
    account_name,
    handle,
    party,
    created_at,
    year_week,
    phase,
    text
FROM posts_prepared;
"""


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Ausgabeordner existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_prepared_posts(database_path: Path) -> pd.DataFrame:
    """
    Lädt die vorbereiteten Posts aus der Datenbank.
    """
    with sqlite3.connect(database_path) as connection:
        df = pd.read_sql_query(SELECT_PREPARED_POSTS_SQL, connection)

    return df


def calculate_sentiment_score(text: str) -> float:
    """
    Berechnet einen einfachen Sentiment-Score im Bereich von -1 bis 1.
    """
    if not isinstance(text, str) or not text.strip():
        return 0.0

    blob = TextBlob(text)
    return float(blob.sentiment.polarity)


def classify_sentiment(score: float) -> str:
    """
    Ordnet einen Score in eine Sentiment-Klasse ein.
    """
    if score > 0.1:
        return "positive"
    if score < -0.1:
        return "negative"
    return "neutral"


def add_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ergänzt Sentiment-Score und Sentiment-Klasse pro Post.
    """
    df = df.copy()
    df["sentiment_score"] = df["text"].apply(calculate_sentiment_score)
    df["sentiment_label"] = df["sentiment_score"].apply(classify_sentiment)
    return df


def aggregate_by_phase(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregiert Sentiment-Kennzahlen je Phase.
    """
    phase_df = (
        df.groupby("phase", as_index=False)
        .agg(
            post_count=("post_id", "count"),
            avg_sentiment=("sentiment_score", "mean"),
            positive_posts=("sentiment_label", lambda s: (s == "positive").sum()),
            neutral_posts=("sentiment_label", lambda s: (s == "neutral").sum()),
            negative_posts=("sentiment_label", lambda s: (s == "negative").sum()),
        )
        .sort_values("phase")
        .reset_index(drop=True)
    )

    return phase_df


def aggregate_by_account(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregiert Sentiment-Kennzahlen je Account.
    """
    account_df = (
        df.groupby(["party", "account_name", "handle"], as_index=False)
        .agg(
            post_count=("post_id", "count"),
            avg_sentiment=("sentiment_score", "mean"),
            positive_posts=("sentiment_label", lambda s: (s == "positive").sum()),
            neutral_posts=("sentiment_label", lambda s: (s == "neutral").sum()),
            negative_posts=("sentiment_label", lambda s: (s == "negative").sum()),
        )
        .sort_values(["party", "account_name"])
        .reset_index(drop=True)
    )

    return account_df


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    """
    Speichert ein DataFrame als CSV-Datei.
    """
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


def main() -> None:
    """
    Führt die Sentiment-Analyse vollständig aus.
    """
    ensure_output_directory()

    print("Lade vorbereitete Posts ...")
    df = load_prepared_posts(DATABASE_PATH)
    print(f"Geladene Zeilen: {len(df)}")

    print("Berechne Sentiment pro Post ...")
    df = add_sentiment(df)

    print("Aggregiere Sentiment je Phase ...")
    phase_df = aggregate_by_phase(df)

    print("Aggregiere Sentiment je Account ...")
    account_df = aggregate_by_account(df)

    print("Speichere Ergebnisse ...")
    save_dataframe(df, POST_LEVEL_OUTPUT)
    save_dataframe(phase_df, PHASE_OUTPUT)
    save_dataframe(account_df, ACCOUNT_OUTPUT)

    print("Sentiment-Analyse abgeschlossen.")
    print(f"- {POST_LEVEL_OUTPUT}")
    print(f"- {PHASE_OUTPUT}")
    print(f"- {ACCOUNT_OUTPUT}")


if __name__ == "__main__":
    main()