"""
03_fetch_user_ids.py

Lädt für eine definierte Liste politischer X-Accounts die
zugehörigen User-IDs über die offizielle X API und speichert
das Ergebnis als CSV-Datei.

Ziel:
Eine stabile Grundlage für den späteren Post-Abruf schaffen.
"""

from pathlib import Path
import os
import requests
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

OUTPUT_DIR = Path("data") / "raw"
OUTPUT_PATH = OUTPUT_DIR / "account_lookup.csv"

BASE_URL = "https://api.x.com/2/users/by/username"


ACCOUNTS = [
    {"account_name": "Friedrich Merz", "handle": "_FriedrichMerz", "party": "CDU/CSU"},
    {"account_name": "Markus Söder", "handle": "Markus_Soeder", "party": "CDU/CSU"},
    {"account_name": "Olaf Scholz", "handle": "OlafScholz", "party": "SPD"},
    {"account_name": "Lars Klingbeil", "handle": "larsklingbeil", "party": "SPD"},
    {"account_name": "Robert Habeck", "handle": "roberthabeck", "party": "Bündnis 90/Die Grünen"},
    {"account_name": "Christian Lindner", "handle": "c_lindner", "party": "FDP"},
    {"account_name": "Alice Weidel", "handle": "Alice_Weidel", "party": "AfD"},
    {"account_name": "Sahra Wagenknecht", "handle": "SWagenknecht", "party": "BSW"},
    {"account_name": "Jan van Aken", "handle": "jan_vanaken", "party": "Die Linke"},
    {"account_name": "Heidi Reichinnek", "handle": "HeidiReichinnek", "party": "Die Linke"},
]


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Ausgabeordner existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_bearer_token() -> str:
    """
    Liest den Bearer Token aus der .env-Datei.
    """
    token = os.getenv("X_BEARER_TOKEN")

    if not token:
        raise ValueError(
            "Kein Bearer Token gefunden. Bitte X_BEARER_TOKEN in der .env setzen."
        )

    return token


def build_headers(bearer_token: str) -> dict:
    """
    Baut den Authorization Header für die X API.
    """
    return {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "election-x-analysis"
    }


def fetch_user_id(handle: str, headers: dict) -> dict:
    """
    Ruft die User-ID eines Handles über die X API ab.
    """
    url = f"{BASE_URL}/{handle}"

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code != 200:
        return {
            "handle": handle,
            "user_id": None,
            "username_from_api": None,
            "name_from_api": None,
            "status_code": response.status_code,
            "error": response.text
        }

    payload = response.json()
    data = payload.get("data", {})

    return {
        "handle": handle,
        "user_id": data.get("id"),
        "username_from_api": data.get("username"),
        "name_from_api": data.get("name"),
        "status_code": response.status_code,
        "error": None
    }


def collect_account_lookup() -> pd.DataFrame:
    """
    Holt User-IDs für alle definierten Accounts.
    """
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)

    rows = []

    for account in ACCOUNTS:
        print(f"Rufe User-ID ab für @{account['handle']} ...")
        result = fetch_user_id(account["handle"], headers)

        row = {
            "account_name": account["account_name"],
            "handle": account["handle"],
            "party": account["party"],
            "user_id": result["user_id"],
            "username_from_api": result["username_from_api"],
            "name_from_api": result["name_from_api"],
            "status_code": result["status_code"],
            "error": result["error"],
        }

        rows.append(row)

    return pd.DataFrame(rows)


def main() -> None:
    """
    Führt den User-Lookup vollständig aus.
    """
    ensure_output_directory()

    print("Starte Account-Lookup über die X API ...")
    df = collect_account_lookup()

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print("Lookup abgeschlossen.")
    print(f"Datei gespeichert unter: {OUTPUT_PATH}")
    print(df[["account_name", "handle", "user_id", "status_code"]])


if __name__ == "__main__":
    main()