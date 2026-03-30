"""
12_communication_mode_analysis.py

Ordnet vorbereitete Posts regelbasiert einem Kommunikationsmodus zu.

Ziele:
- Kommunikationsmodus pro Post bestimmen
- Ergebnisse je Phase und je Account aggregieren
- Analyseergebnisse als CSV-Dateien speichern

Kategorien:
- Angriff
- Sachthema
- Mobilisierung
- Sonstiges

Hinweis:
Dies ist ein transparenter regelbasierter MVP. Die Kategorisierung
erfolgt über einfache Keyword-Listen und dient als erste
strukturierte Annäherung an Kommunikationsmuster.
"""

from pathlib import Path
import sqlite3
import pandas as pd


DATABASE_PATH = Path("database") / "election_posts.db"
OUTPUT_DIR = Path("data") / "processed"

POST_LEVEL_OUTPUT = OUTPUT_DIR / "communication_mode_post_level.csv"
PHASE_OUTPUT = OUTPUT_DIR / "communication_mode_by_phase.csv"
ACCOUNT_OUTPUT = OUTPUT_DIR / "communication_mode_by_account.csv"


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


ATTACK_KEYWORDS = [
    "schuld", "versagen", "gescheitert", "falsch", "lüge", "lügen",
    "unfähig", "chaos", "katastrophe", "verantwortungslos", "kritik",
    "angriff", "skandal", "versagt", "problem"
]

POLICY_KEYWORDS = [
    "wirtschaft", "steuern", "migration", "sicherheit", "rente",
    "klima", "bildung", "haushalt", "energie", "arbeitsmarkt",
    "gesundheit", "wachstum", "inflation", "sozial", "investition",
    "gesetz", "reform", "politik", "maßnahmen", "programm"
]

MOBILIZATION_KEYWORDS = [
    "wählt", "wählen", "wahl", "stimme", "unterstützt", "unterstützen",
    "gemeinsam", "kommt", "kommen", "jetzt", "mitmachen", "dabei",
    "kampf", "wahlkampf", "gehen", "zukunft", "für euch", "für dich"
]


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Ausgabeordner existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_prepared_posts(database_path: Path) -> pd.DataFrame:
    """
    Lädt vorbereitete Posts aus der Datenbank.
    """
    with sqlite3.connect(database_path) as connection:
        df = pd.read_sql_query(SELECT_PREPARED_POSTS_SQL, connection)

    return df


def count_keyword_matches(text: str, keywords: list[str]) -> int:
    """
    Zählt einfache Keyword-Treffer im Text.
    """
    if not isinstance(text, str):
        return 0

    text_lower = text.lower()
    return sum(1 for keyword in keywords if keyword in text_lower)


def classify_communication_mode(text: str) -> str:
    """
    Ordnet einen Post einem Kommunikationsmodus zu.
    """
    attack_score = count_keyword_matches(text, ATTACK_KEYWORDS)
    policy_score = count_keyword_matches(text, POLICY_KEYWORDS)
    mobilization_score = count_keyword_matches(text, MOBILIZATION_KEYWORDS)

    scores = {
        "Angriff": attack_score,
        "Sachthema": policy_score,
        "Mobilisierung": mobilization_score,
    }

    best_label = max(scores, key=scores.get)
    best_score = scores[best_label]

    if best_score == 0:
        return "Sonstiges"

    # Falls Gleichstand zwischen mehreren Kategorien besteht,
    # wird der Post als Sonstiges markiert, um Überinterpretation zu vermeiden.
    top_count = sum(1 for score in scores.values() if score == best_score)
    if top_count > 1:
        return "Sonstiges"

    return best_label


def add_communication_mode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ergänzt die Kommunikationsmodus-Kategorie pro Post.
    """
    df = df.copy()
    df["communication_mode"] = df["text"].apply(classify_communication_mode)
    return df


def aggregate_by_phase(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregiert Kommunikationsmodi je Phase.
    """
    phase_df = (
        df.groupby(["phase", "communication_mode"], as_index=False)
        .agg(post_count=("post_id", "count"))
        .sort_values(["phase", "communication_mode"])
        .reset_index(drop=True)
    )

    return phase_df


def aggregate_by_account(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregiert Kommunikationsmodi je Account.
    """
    account_df = (
        df.groupby(
            ["party", "account_name", "handle", "communication_mode"],
            as_index=False
        )
        .agg(post_count=("post_id", "count"))
        .sort_values(["party", "account_name", "communication_mode"])
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
    Führt die Kommunikationsmodus-Analyse vollständig aus.
    """
    ensure_output_directory()

    print("Lade vorbereitete Posts ...")
    df = load_prepared_posts(DATABASE_PATH)
    print(f"Geladene Zeilen: {len(df)}")

    print("Ordne Kommunikationsmodi zu ...")
    df = add_communication_mode(df)

    print("Aggregiere Kommunikationsmodi je Phase ...")
    phase_df = aggregate_by_phase(df)

    print("Aggregiere Kommunikationsmodi je Account ...")
    account_df = aggregate_by_account(df)

    print("Speichere Ergebnisse ...")
    save_dataframe(df, POST_LEVEL_OUTPUT)
    save_dataframe(phase_df, PHASE_OUTPUT)
    save_dataframe(account_df, ACCOUNT_OUTPUT)

    print("Kommunikationsmodus-Analyse abgeschlossen.")
    print(f"- {POST_LEVEL_OUTPUT}")
    print(f"- {PHASE_OUTPUT}")
    print(f"- {ACCOUNT_OUTPUT}")


if __name__ == "__main__":
    main()