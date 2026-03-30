"""
04_collect_x_posts_api.py

Ruft Posts der ausgewählten politischen X-Accounts über die offizielle X API ab
und speichert sie direkt in die SQLite-Datenbank.

Ziel:
Echte Rohdaten für den definierten Untersuchungszeitraum sammeln und in das
bestehende Schema der Tabelle `posts` überführen.

Wichtige Verbesserung:
Das Skript validiert den Account-Lookup strikt, verarbeitet nur gültige User-IDs
und prüft am Ende, ob wirklich alle erwarteten Zielaccounts erfolgreich in die
Datenerhebung eingegangen sind.
"""

from pathlib import Path
import os
import sqlite3
import sys
import time
from typing import Any

import requests
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

DATABASE_PATH = Path("database") / "election_posts.db"
ACCOUNT_LOOKUP_PATH = Path("data") / "raw" / "account_lookup.csv"

BASE_URL_TEMPLATE = "https://api.x.com/2/users/{user_id}/tweets"

START_TIME = "2024-11-06T00:00:00Z"
END_TIME = "2025-02-24T00:00:00Z"

MAX_RESULTS = 100
REQUEST_TIMEOUT_SECONDS = 60
EXPECTED_ACCOUNT_COUNT = 10
REQUEST_PAUSE_SECONDS = 1

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


def get_bearer_token() -> str:
    """
    Liest den Bearer Token aus der .env-Datei.
    """
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        raise ValueError("Kein Bearer Token gefunden. Bitte X_BEARER_TOKEN in der .env setzen.")
    return token


def build_headers(bearer_token: str) -> dict[str, str]:
    """
    Baut den Authorization Header für die X API.
    """
    return {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "election-x-analysis"
    }


def load_account_lookup(csv_path: Path) -> pd.DataFrame:
    """
    Lädt die Lookup-Datei und prüft, ob sie vorhanden ist.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Account-Lookup-Datei nicht gefunden: {csv_path}")
    return pd.read_csv(csv_path)


def validate_account_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prüft den Lookup auf Vollständigkeit und filtert nur valide Accounts heraus.
    """
    required_columns = {
        "account_name",
        "handle",
        "party",
        "user_id",
        "status_code",
        "error",
        "lookup_ok",
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(
            "In account_lookup.csv fehlen Pflichtspalten: "
            + ", ".join(sorted(missing_columns))
        )

    if len(df) != EXPECTED_ACCOUNT_COUNT:
        print(
            f"WARNUNG: account_lookup.csv enthält {len(df)} Zeilen, "
            f"erwartet werden {EXPECTED_ACCOUNT_COUNT}."
        )

    handles = df["handle"].astype(str).str.strip().tolist()
    duplicate_handles = sorted({handle for handle in handles if handles.count(handle) > 1})
    if duplicate_handles:
        raise ValueError(
            "Doppelte Handles im Account-Lookup gefunden: "
            + ", ".join(f"@{handle}" for handle in duplicate_handles)
        )

    df = df.copy()

    df["user_id_str"] = df["user_id"].astype(str).str.strip()
    df["lookup_ok_normalized"] = df["lookup_ok"].astype(str).str.strip().str.lower()

    valid_mask = (
        df["lookup_ok_normalized"].isin(["true", "1"])
        & df["status_code"].fillna(0).astype(int).eq(200)
        & ~df["user_id"].isna()
        & ~df["user_id_str"].isin(["", "nan", "none", "null"])
    )

    valid_df = df.loc[valid_mask].copy()
    invalid_df = df.loc[~valid_mask].copy()

    print(f"Valide Accounts im Lookup: {len(valid_df)}/{len(df)}")

    if not invalid_df.empty:
        print("\nAccounts mit ungültigem oder fehlgeschlagenem Lookup:")
        print(
            invalid_df[
                ["account_name", "handle", "user_id", "status_code", "error", "lookup_ok"]
            ].to_string(index=False)
        )

    if valid_df.empty:
        raise ValueError(
            "Es wurden keine validen Accounts im Lookup gefunden. "
            "Bitte zuerst 03_fetch_user_ids.py erfolgreich ausführen."
        )

    return valid_df


def extract_error_text(response: requests.Response) -> str:
    """
    Extrahiert eine möglichst brauchbare Fehlermeldung aus der API-Antwort.
    """
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = payload.get("detail")
            title = payload.get("title")
            errors = payload.get("errors")

            if detail:
                return str(detail)

            if title:
                return str(title)

            if errors:
                return str(errors)

        return response.text.strip() or "Unbekannter API-Fehler"
    except ValueError:
        return response.text.strip() or "Unbekannter API-Fehler"


def is_reply(tweet: dict[str, Any]) -> int:
    """
    Prüft, ob ein Tweet eine Reply ist.
    """
    referenced = tweet.get("referenced_tweets", [])
    return int(any(ref.get("type") == "replied_to" for ref in referenced))


def is_retweet(tweet: dict[str, Any]) -> int:
    """
    Prüft, ob ein Tweet ein Retweet ist.
    """
    referenced = tweet.get("referenced_tweets", [])
    return int(any(ref.get("type") == "retweeted" for ref in referenced))


def map_tweet_to_row(tweet: dict[str, Any], account_name: str, handle: str, party: str) -> tuple:
    """
    Mappt einen Tweet aus der API in eine DB-Zeile.
    """
    metrics = tweet.get("public_metrics", {})

    return (
        str(tweet.get("id")),
        account_name,
        handle,
        party,
        tweet.get("created_at"),
        tweet.get("text", ""),
        int(metrics.get("like_count", 0)),
        int(metrics.get("retweet_count", 0)),
        int(metrics.get("reply_count", 0)),
        int(metrics.get("quote_count", 0)),
        is_reply(tweet),
        is_retweet(tweet),
        "x_api",
    )


def fetch_posts_for_user(user_id: str, headers: dict[str, str]) -> dict[str, Any]:
    """
    Ruft alle Posts eines Users im definierten Zeitraum über die X API ab.
    """
    all_tweets: list[dict[str, Any]] = []
    next_token = None
    page_count = 0

    while True:
        params = {
            "max_results": MAX_RESULTS,
            "start_time": START_TIME,
            "end_time": END_TIME,
            "tweet.fields": "created_at,public_metrics,referenced_tweets",
            "exclude": "replies,retweets",
        }

        if next_token:
            params["pagination_token"] = next_token

        url = BASE_URL_TEMPLATE.format(user_id=user_id)

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": None,
                "error": f"RequestException: {exc}",
                "pages_fetched": page_count,
            }

        if response.status_code != 200:
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": response.status_code,
                "error": extract_error_text(response),
                "pages_fetched": page_count,
            }

        try:
            payload = response.json()
        except ValueError:
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": response.status_code,
                "error": "Antwort konnte nicht als JSON geparst werden.",
                "pages_fetched": page_count,
            }

        tweets = payload.get("data", [])
        meta = payload.get("meta", {})

        if tweets and not isinstance(tweets, list):
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": response.status_code,
                "error": "API-Antwort enthält kein gültiges Listenformat in 'data'.",
                "pages_fetched": page_count,
            }

        all_tweets.extend(tweets)
        page_count += 1

        next_token = meta.get("next_token")
        if not next_token:
            break

        time.sleep(REQUEST_PAUSE_SECONDS)

    return {
        "ok": True,
        "tweets": all_tweets,
        "status_code": 200,
        "error": None,
        "pages_fetched": page_count,
    }


def insert_rows(rows: list[tuple], database_path: Path) -> int:
    """
    Fügt Zeilen in die Tabelle posts ein und gibt die Anzahl tatsächlich eingefügter Zeilen zurück.
    """
    if not rows:
        return 0

    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        before_changes = connection.total_changes
        cursor.executemany(INSERT_SQL, rows)
        connection.commit()
        after_changes = connection.total_changes

    inserted_count = after_changes - before_changes
    return inserted_count


def fetch_distinct_handles_from_database(database_path: Path) -> set[str]:
    """
    Liest die in posts vorhandenen Handles aus der Datenbank.
    """
    if not database_path.exists():
        raise FileNotFoundError(f"Datenbank nicht gefunden: {database_path}")

    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT handle FROM posts")
        rows = cursor.fetchall()

    return {str(row[0]).strip() for row in rows if row and row[0] is not None}


def main() -> None:
    """
    Führt den vollständigen Post-Abruf aus.
    """
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)

    accounts_df = load_account_lookup(ACCOUNT_LOOKUP_PATH)
    valid_accounts_df = validate_account_lookup(accounts_df)

    total_rows_mapped = 0
    total_rows_inserted = 0
    processed_handles: list[str] = []
    failed_accounts: list[dict[str, Any]] = []
    zero_post_accounts: list[dict[str, Any]] = []

    for _, account in valid_accounts_df.iterrows():
        account_name = str(account["account_name"]).strip()
        handle = str(account["handle"]).strip()
        party = str(account["party"]).strip()
        user_id = str(account["user_id"]).strip()

        print(f"\nRufe Posts ab für @{handle} ({account_name}) | user_id={user_id} ...")
        result = fetch_posts_for_user(user_id, headers)

        if not result["ok"]:
            print(
                f"FEHLER beim Abruf für @{handle}: "
                f"status_code={result['status_code']} | error={result['error']}"
            )
            failed_accounts.append(
                {
                    "account_name": account_name,
                    "handle": handle,
                    "user_id": user_id,
                    "status_code": result["status_code"],
                    "error": result["error"],
                }
            )
            time.sleep(REQUEST_PAUSE_SECONDS)
            continue

        tweets = result["tweets"]
        rows = [
            map_tweet_to_row(tweet, account_name, handle, party)
            for tweet in tweets
        ]

        inserted_count = insert_rows(rows, DATABASE_PATH)

        total_rows_mapped += len(rows)
        total_rows_inserted += inserted_count
        processed_handles.append(handle)

        print(
            f"Erfolgreich verarbeitet für @{handle}: "
            f"{len(rows)} Posts abgerufen | {inserted_count} neue Zeilen gespeichert | "
            f"Seiten: {result['pages_fetched']}"
        )

        if len(rows) == 0:
            zero_post_accounts.append(
                {
                    "account_name": account_name,
                    "handle": handle,
                    "user_id": user_id,
                }
            )
            print(
                f"WARNUNG: Für @{handle} wurden 0 Posts im definierten Zeitraum zurückgegeben."
            )

        time.sleep(REQUEST_PAUSE_SECONDS)

    expected_handles = set(valid_accounts_df["handle"].astype(str).str.strip().tolist())
    db_handles = fetch_distinct_handles_from_database(DATABASE_PATH)
    missing_in_database = sorted(expected_handles - db_handles)

    print("\nDatenerhebung abgeschlossen.")
    print(f"Valide Zielaccounts aus Lookup: {len(valid_accounts_df)}")
    print(f"Erfolgreich technisch verarbeitete Accounts: {len(processed_handles)}")
    print(f"Insgesamt abgerufene Posts: {total_rows_mapped}")
    print(f"Insgesamt neu gespeicherte Posts: {total_rows_inserted}")
    print(f"Handles in Tabelle posts: {len(db_handles)}")

    if failed_accounts:
        print("\nAccounts mit technischem Abruffehler:")
        print(pd.DataFrame(failed_accounts).to_string(index=False))

    if zero_post_accounts:
        print("\nAccounts mit 0 Posts im Untersuchungszeitraum:")
        print(pd.DataFrame(zero_post_accounts).to_string(index=False))

    if missing_in_database:
        print("\nFEHLENDE Handles in Tabelle posts:")
        for handle in missing_in_database:
            print(f"- @{handle}")

    run_success = (
        len(valid_accounts_df) == EXPECTED_ACCOUNT_COUNT
        and not failed_accounts
        and not missing_in_database
    )

    if not run_success:
        print(
            "\nABBRUCH: Die Datenerhebung ist unvollständig. "
            "Bitte Lookup und API-Abruf prüfen, bevor die Pipeline fortgesetzt wird."
        )
        sys.exit(1)

    print(
        "\nAlle erwarteten Zielaccounts wurden erfolgreich in die Datenerhebung übernommen."
    )


if __name__ == "__main__":
    main()