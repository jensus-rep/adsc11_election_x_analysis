"""
01_scrape_posts.py

EXPERIMENTELLER PROTOTYP

Erster Test für den Zugriff auf X mit twikit.
Der Ansatz wird aktuell nicht produktiv verwendet, da der Zugriff
durch Cloudflare / Plattformrestriktionen blockiert wird.

Das Skript bleibt zur Dokumentation des Entwicklungsprozesses erhalten.

*******

Zweck:
Überprüfung, ob der Zugriff auf X funktioniert und Tweets eines
bestimmten Accounts geladen werden können.

Hinweis:
Die Zugangsdaten werden aus Umgebungsvariablen gelesen, damit
keine sensiblen Daten im Repository gespeichert werden.
"""

import asyncio
import os
from twikit import Client


async def fetch_posts(username: str, limit: int = 5) -> None:
    """
    Ruft eine begrenzte Anzahl von Posts eines Accounts ab
    und gibt diese im Terminal aus.

    Parameter
    ----------
    username : str
        X-Handle des Accounts (ohne @)
    limit : int
        Anzahl der anzuzeigenden Tweets
    """

    client = Client(language="en-US")

    # Login-Daten aus Umgebungsvariablen lesen
    x_username = os.getenv("X_USERNAME")
    x_password = os.getenv("X_PASSWORD")

    if not all([x_username, x_password]):
        raise ValueError(
            "Login-Daten fehlen. Bitte Umgebungsvariablen setzen: "
            "X_USERNAME, X_EMAIL, X_PASSWORD"
        )

    # Login bei X
    await client.login(
        auth_info_1=x_username,
        password=x_password
    )

    # Tweets abrufen
    tweets = await client.get_user_tweets(username, "Tweets")

    # Ausgabe der ersten Tweets
    for i, tweet in enumerate(tweets):
        if i >= limit:
            break

        print("ID:", tweet.id)
        print("Datum:", tweet.created_at)
        print("Text:", tweet.text)
        print("-" * 60)


if __name__ == "__main__":
    asyncio.run(fetch_posts("OlafScholz"))