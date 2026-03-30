"""
02_import_posts.py

Importiert Posts aus einer CSV-Datei in die SQLite-Datenbank.

Ziel:
Eine quellenunabhängige Importschicht schaffen, sodass Daten
aus CSV-Dateien standardisiert in die Tabelle `posts` geladen
werden können.

Hinweis:
Die CSV-Datei sollte idealerweise die Spalten enthalten, die
dem Datenbankschema entsprechen.
"""

from pathlib import Path
import sqlite3
import pandas as pd


DATABASE_PATH = Path("database") / "election_posts.db"
DEFAULT_CSV_PATH = Path("data") / "raw" / "posts.csv"


REQUIRED_COLUMNS = [
    "post_id",
    "account_name",
    "handle",
    "created_at",
    "text",
]

OPTIONAL_COLUMNS_WITH_DEFAULTS = {
    "party": None,
    "like_count": 0,
    "retweet_count": 0,
    "reply_count": 0,
    "quote_count": 0,
    "is_reply": 0,
    "is_retweet": 0,
    "source": "csv_import",
}


INSERT_SQL = """
INSERT OR IGNORE INTO posts (
    post_id,
    account_name,
    handle,
    party,
    created_at,
    text,
    like_count,
    retweet_count,
    reply_count,
    quote_count,
    is_reply,
    is_retweet,
    source
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def load_csv(csv_path: Path) -> pd.DataFrame:
    """
    Lädt eine CSV-Datei in ein DataFrame.

    Unterstützt:
    - UTF-8 und Windows-1252 Encoding
    - Komma oder Semikolon als Separator
    """

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV-Datei nicht gefunden: {csv_path}")

    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except UnicodeDecodeError:
        print("UTF-8 fehlgeschlagen, versuche Windows-1252 Encoding...")
        df = pd.read_csv(csv_path, encoding="cp1252")

    # Wenn nur eine Spalte erkannt wurde, wahrscheinlich falscher Separator
    if len(df.columns) == 1:
        print("CSV scheint Semikolon-separiert zu sein. Lese erneut mit ';' Separator.")
        try:
            df = pd.read_csv(csv_path, encoding="utf-8", sep=";")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, encoding="cp1252", sep=";")

    return df


def validate_required_columns(df: pd.DataFrame) -> None:
    """
    Prüft, ob alle Pflichtspalten vorhanden sind.
    """
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Fehlende Pflichtspalten in CSV-Datei: {', '.join(missing_columns)}"
        )


def add_optional_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ergänzt optionale Spalten mit Default-Werten, falls sie fehlen.
    """
    for column, default_value in OPTIONAL_COLUMNS_WITH_DEFAULTS.items():
        if column not in df.columns:
            df[column] = default_value

    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bereinigt und standardisiert das DataFrame für den Import.
    """
    validate_required_columns(df)
    df = add_optional_columns(df)

    # Reihenfolge passend zum INSERT
    df = df[
        [
            "post_id",
            "account_name",
            "handle",
            "party",
            "created_at",
            "text",
            "like_count",
            "retweet_count",
            "reply_count",
            "quote_count",
            "is_reply",
            "is_retweet",
            "source",
        ]
    ].copy()

    # Fehlende Textwerte vermeiden
    df["text"] = df["text"].fillna("").astype(str)
    df["account_name"] = df["account_name"].fillna("").astype(str)
    df["handle"] = df["handle"].fillna("").astype(str)
    df["created_at"] = df["created_at"].fillna("").astype(str)

    # Numerische Spalten absichern
    numeric_columns = [
        "like_count",
        "retweet_count",
        "reply_count",
        "quote_count",
        "is_reply",
        "is_retweet",
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    return df


def import_posts(df: pd.DataFrame, database_path: Path) -> int:
    """
    Importiert Datensätze in die SQLite-Datenbank.

    Returns
    -------
    int
        Anzahl der importierten Zeilen laut DataFrame-Länge.
        Durch INSERT OR IGNORE können effektiv weniger neue Zeilen entstehen.
    """
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        rows = list(df.itertuples(index=False, name=None))
        cursor.executemany(INSERT_SQL, rows)
        connection.commit()

    return len(df)


def main() -> None:
    """
    Führt den CSV-Import in die SQLite-Datenbank aus.
    """
    csv_path = DEFAULT_CSV_PATH

    print(f"Lade CSV-Datei: {csv_path}")
    df = load_csv(csv_path)

    print("Prüfe und normalisiere Daten ...")
    df = normalize_dataframe(df)

    print(f"Importiere {len(df)} Zeilen in die Datenbank ...")
    imported_count = import_posts(df, DATABASE_PATH)

    print(f"Import abgeschlossen. Verarbeitete Zeilen: {imported_count}")


if __name__ == "__main__":
    main()