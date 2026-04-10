from pathlib import Path
import sqlite3
import pandas as pd


DATABASE_PATH = Path("database") / "election_posts.db"
OUTPUT_DIR = Path("data/export")
OUTPUT_FILE = OUTPUT_DIR / "posts_prepared_final.csv"

SELECT_SQL = """
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
    phase,
    text,
    like_count,
    retweet_count,
    reply_count,
    quote_count,
    is_reply,
    is_retweet,
    source
FROM posts_prepared
ORDER BY created_at ASC;
"""


def export_posts_prepared_to_csv(database_path: Path, output_file: Path) -> int:
    if not database_path.exists():
        raise FileNotFoundError(f"Datenbank nicht gefunden: {database_path}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(database_path) as connection:
        df = pd.read_sql_query(SELECT_SQL, connection)

    if df.empty:
        raise ValueError("Die Tabelle posts_prepared enthält keine Daten.")

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    return len(df)


def main() -> None:
    print("Starte CSV Export aus posts_prepared ...")
    row_count = export_posts_prepared_to_csv(DATABASE_PATH, OUTPUT_FILE)
    print(f"Export abgeschlossen. Zeilen exportiert: {row_count}")
    print(f"Datei gespeichert unter: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()