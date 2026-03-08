"""
05_analysis_activity.py

Analysiert die Posting-Aktivität der politischen Accounts
auf Basis der vorbereiteten Tabelle `posts_prepared`.

Ziele:
- Posts pro Woche und Account berechnen
- Posts pro Phase und Account berechnen
- Gesamtaggregationen für den späteren Report erzeugen
- Ergebnisse als CSV-Dateien speichern
"""

from pathlib import Path
import sqlite3
import pandas as pd


DATABASE_PATH = Path("database") / "election_posts.db"
OUTPUT_DIR = Path("data") / "processed"

WEEKLY_ACTIVITY_CSV = OUTPUT_DIR / "activity_by_week.csv"
PHASE_ACTIVITY_CSV = OUTPUT_DIR / "activity_by_phase.csv"
TOTAL_ACTIVITY_CSV = OUTPUT_DIR / "activity_total.csv"


SELECT_PREPARED_POSTS_SQL = """
SELECT
    post_id,
    account_name,
    handle,
    party,
    created_at,
    date_only,
    year,
    week,
    year_week,
    phase
FROM posts_prepared;
"""


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Ausgabeordner existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_prepared_posts(database_path: Path) -> pd.DataFrame:
    """
    Lädt die vorbereiteten Post-Daten aus der SQLite-Datenbank.
    """
    with sqlite3.connect(database_path) as connection:
        df = pd.read_sql_query(SELECT_PREPARED_POSTS_SQL, connection)

    return df


def calculate_weekly_activity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Berechnet die Anzahl der Posts pro Woche und Account.
    """
    weekly_df = (
        df.groupby(["party", "account_name", "handle", "year_week"], as_index=False)
        .agg(post_count=("post_id", "count"))
        .sort_values(["year_week", "party", "account_name"])
        .reset_index(drop=True)
    )

    return weekly_df


def calculate_phase_activity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Berechnet die Anzahl der Posts pro Phase und Account.
    """
    phase_df = (
        df.groupby(["phase", "party", "account_name", "handle"], as_index=False)
        .agg(post_count=("post_id", "count"))
        .sort_values(["phase", "party", "account_name"])
        .reset_index(drop=True)
    )

    return phase_df


def calculate_total_activity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Berechnet die gesamte Posting-Aktivität pro Account
    über den gesamten Untersuchungszeitraum.
    """
    total_df = (
        df.groupby(["party", "account_name", "handle"], as_index=False)
        .agg(total_posts=("post_id", "count"))
        .sort_values(["party", "account_name"])
        .reset_index(drop=True)
    )

    return total_df


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    """
    Speichert ein DataFrame als CSV-Datei.
    """
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


def main() -> None:
    """
    Führt die Aktivitätsanalyse vollständig aus.
    """
    ensure_output_directory()

    print("Lade vorbereitete Daten ...")
    df = load_prepared_posts(DATABASE_PATH)
    print(f"Geladene Zeilen: {len(df)}")

    print("Berechne Aktivität pro Woche und Account ...")
    weekly_df = calculate_weekly_activity(df)

    print("Berechne Aktivität pro Phase und Account ...")
    phase_df = calculate_phase_activity(df)

    print("Berechne Gesamtaktivität pro Account ...")
    total_df = calculate_total_activity(df)

    print("Speichere Ergebnisse als CSV ...")
    save_dataframe(weekly_df, WEEKLY_ACTIVITY_CSV)
    save_dataframe(phase_df, PHASE_ACTIVITY_CSV)
    save_dataframe(total_df, TOTAL_ACTIVITY_CSV)

    print("Aktivitätsanalyse abgeschlossen.")
    print(f"- {WEEKLY_ACTIVITY_CSV}")
    print(f"- {PHASE_ACTIVITY_CSV}")
    print(f"- {TOTAL_ACTIVITY_CSV}")


if __name__ == "__main__":
    main()