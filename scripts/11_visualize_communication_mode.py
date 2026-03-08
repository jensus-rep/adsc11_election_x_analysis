"""
11_visualize_communication_mode.py

Erstellt Visualisierungen auf Basis der Kommunikationsmodus-Analyse.

Ziele:
- Verteilung der Kommunikationsmodi nach Phase darstellen
- Verteilung der Kommunikationsmodi je Account darstellen

Eingabedateien:
- data/processed/communication_mode_by_phase.csv
- data/processed/communication_mode_by_account.csv

Ausgabedateien:
- figures/communication_mode_by_phase.png
- figures/communication_mode_by_account.png
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


INPUT_DIR = Path("data") / "processed"
OUTPUT_DIR = Path("figures")

PHASE_INPUT = INPUT_DIR / "communication_mode_by_phase.csv"
ACCOUNT_INPUT = INPUT_DIR / "communication_mode_by_account.csv"

PHASE_OUTPUT = OUTPUT_DIR / "communication_mode_by_phase.png"
ACCOUNT_OUTPUT = OUTPUT_DIR / "communication_mode_by_account.png"


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Zielordner für Grafiken existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lädt die Kommunikationsmodus-Daten.
    """
    phase_df = pd.read_csv(PHASE_INPUT)
    account_df = pd.read_csv(ACCOUNT_INPUT)
    return phase_df, account_df


def plot_mode_by_phase(phase_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik der Kommunikationsmodi nach Phase.
    """
    pivot_df = phase_df.pivot_table(
        index="phase",
        columns="communication_mode",
        values="post_count",
        fill_value=0
    ).copy()

    preferred_order = ["Angriff", "Sachthema", "Mobilisierung", "Sonstiges"]
    existing_columns = [col for col in preferred_order if col in pivot_df.columns]
    pivot_df = pivot_df[existing_columns]

    ax = pivot_df.plot(kind="bar", figsize=(10, 6))
    ax.set_title("Kommunikationsmodi nach Phase")
    ax.set_xlabel("Phase")
    ax.set_ylabel("Anzahl der Posts")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(PHASE_OUTPUT, dpi=300, bbox_inches="tight")
    plt.close()


def plot_mode_by_account(account_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik der Kommunikationsmodi je Account.
    """
    pivot_df = account_df.pivot_table(
        index="account_name",
        columns="communication_mode",
        values="post_count",
        fill_value=0
    ).copy()

    preferred_order = ["Angriff", "Sachthema", "Mobilisierung", "Sonstiges"]
    existing_columns = [col for col in preferred_order if col in pivot_df.columns]
    pivot_df = pivot_df[existing_columns]

    pivot_df["total_posts"] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.sort_values("total_posts", ascending=False)
    pivot_df = pivot_df.drop(columns=["total_posts"])

    ax = pivot_df.plot(kind="bar", stacked=True, figsize=(12, 7))
    ax.set_title("Kommunikationsmodi je Account")
    ax.set_xlabel("Account")
    ax.set_ylabel("Anzahl der Posts")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(ACCOUNT_OUTPUT, dpi=300, bbox_inches="tight")
    plt.close()


def main() -> None:
    """
    Führt die Visualisierung der Kommunikationsmodi vollständig aus.
    """
    ensure_output_directory()

    print("Lade Kommunikationsmodus-Daten ...")
    phase_df, account_df = load_data()

    print("Erstelle Grafik: Kommunikationsmodi nach Phase ...")
    plot_mode_by_phase(phase_df)

    print("Erstelle Grafik: Kommunikationsmodi je Account ...")
    plot_mode_by_account(account_df)

    print("Visualisierung abgeschlossen.")
    print(f"- {PHASE_OUTPUT}")
    print(f"- {ACCOUNT_OUTPUT}")


if __name__ == "__main__":
    main()