"""
01_scrape_posts.py

Testscript zum Abrufen einiger Posts eines X-Accounts.
Dient nur zur Überprüfung, ob der Zugriff funktioniert.
"""

import asyncio
from twikit import Client


async def fetch_posts(username, limit=5):
    client = Client(language="en-US")

    # Tweets abrufen
    tweets = await client.get_user_tweets(username, "Tweets")

    for i, tweet in enumerate(tweets):
        if i >= limit:
            break

        print("ID:", tweet.id)
        print("Datum:", tweet.created_at)
        print("Text:", tweet.text)
        print("-" * 40)


if __name__ == "__main__":
    asyncio.run(fetch_posts("OlafScholz"))