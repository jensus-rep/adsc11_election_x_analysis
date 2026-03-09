"""
07_collect_x_posts_api.py

Ruft Posts der ausgewählten politischen X-Accounts über die offizielle X API ab
und speichert sie direkt in die SQLite-Datenbank.

Ziel:
Echte Rohdaten für den definierten Untersuchungszeitraum sammeln und in das
bestehende Schema der Tabelle `posts` überführen.
"""

from pathlib import Path
import os
import sqlite3
import time
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
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        raise ValueError("Kein Bearer Token gefunden. Bitte X_BEARER_TOKEN in der .env setzen.")
    return token


def build_headers(bearer_token: str) -> dict:
    return {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "election-x-analysis"
    }


def load_account_lookup(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Account-Lookup-Datei nicht gefunden: {csv_path}")
    return pd.read_csv(csv_path)


def is_reply(tweet: dict) -> int:
    referenced = tweet.get("referenced_tweets", [])
    return int(any(ref.get("type") == "replied_to" for ref in referenced))


def is_retweet(tweet: dict) -> int:
    referenced = tweet.get("referenced_tweets", [])
    return int(any(ref.get("type") == "retweeted" for ref in referenced))


def map_tweet_to_row(tweet: dict, account_name: str, handle: str, party: str) -> tuple:
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


def fetch_posts_for_user(user_id: str, headers: dict) -> list[dict]:
    all_tweets = []
    next_token = None

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
        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code != 200:
            print(f"Fehler beim Abruf für user_id={user_id}: {response.status_code}")
            print(response.text)
            break

        payload = response.json()
        tweets = payload.get("data", [])
        meta = payload.get("meta", {})

        all_tweets.extend(tweets)

        next_token = meta.get("next_token")
        if not next_token:
            break

        time.sleep(1)

    return all_tweets


def insert_rows(rows: list[tuple], database_path: Path) -> None:
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.executemany(INSERT_SQL, rows)
        connection.commit()


def main() -> None:
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)

    accounts_df = load_account_lookup(ACCOUNT_LOOKUP_PATH)

    total_rows = 0

    for _, account in accounts_df.iterrows():
        account_name = account["account_name"]
        handle = account["handle"]
        party = account["party"]
        user_id = str(account["user_id"])

        print(f"Rufe Posts ab für @{handle} ({account_name}) ...")
        tweets = fetch_posts_for_user(user_id, headers)

        rows = [
            map_tweet_to_row(tweet, account_name, handle, party)
            for tweet in tweets
        ]

        insert_rows(rows, DATABASE_PATH)
        total_rows += len(rows)

        print(f"Gespeicherte Posts für @{handle}: {len(rows)}")
        time.sleep(1)

    print(f"Datenerhebung abgeschlossen. Insgesamt verarbeitete Posts: {total_rows}")


if __name__ == "__main__":
    main()