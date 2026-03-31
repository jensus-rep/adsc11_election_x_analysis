"""
13_visualize_communication_mode.py

Erstellt Visualisierungen auf Basis der Kommunikationsmodus-Analyse.

Ziele:
- Verteilung der Kommunikationsmodi nach Phase als Prozentanteile darstellen
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

PREFERRED_MODE_ORDER = ["Angriff", "Sachthema", "Mobilisierung", "Sonstiges"]
PHASE_ORDER = ["Phase A", "Phase B"]

FIGSIZE_PHASE = (10, 6)
FIGSIZE_ACCOUNT = (12, 7)
DPI = 300

TITLE_SIZE = 18
LABEL_SIZE = 13
TICK_SIZE = 11
LEGEND_SIZE = 11


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


def reorder_columns(existing_columns: list[str]) -> list[str]:
    """
    Ordnet Kommunikationsmodi in fachlich gewünschter Reihenfolge.
    """
    return [col for col in PREFERRED_MODE_ORDER if col in existing_columns]


def add_bar_labels(ax: plt.Axes, decimals: int = 1, suffix: str = "%") -> None:
    """
    Schreibt Werte oberhalb der Balken auf die Grafik.
    """
    for container in ax.containers:
        labels = []
        for bar in container:
            height = bar.get_height()
            if pd.isna(height):
                labels.append("")
            else:
                labels.append(f"{height:.{decimals}f}{suffix}")
        ax.bar_label(container, labels=labels, padding=3, fontsize=10)


def plot_mode_by_phase(phase_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik der Kommunikationsmodi nach Phase
    auf Basis prozentualer Anteile innerhalb jeder Phase.
    """
    pivot_df = phase_df.pivot_table(
        index="phase",
        columns="communication_mode",
        values="post_count",
        fill_value=0
    ).copy()

    for phase_name in PHASE_ORDER:
        if phase_name not in pivot_df.index:
            pivot_df.loc[phase_name] = 0

    pivot_df = pivot_df.loc[PHASE_ORDER]

    ordered_columns = reorder_columns(pivot_df.columns.tolist())
    pivot_df = pivot_df[ordered_columns]

    phase_totals = pivot_df.sum(axis=1)
    percentage_df = pivot_df.div(phase_totals.replace(0, pd.NA), axis=0) * 100
    percentage_df = percentage_df.fillna(0)

    ax = percentage_df.plot(kind="bar", figsize=FIGSIZE_PHASE)

    ax.set_title("Kommunikationsmodi nach Phase", fontsize=TITLE_SIZE, pad=12)
    ax.set_xlabel("Phase", fontsize=LABEL_SIZE)
    ax.set_ylabel("Anteil an allen Posts in Prozent", fontsize=LABEL_SIZE)
    ax.tick_params(axis="x", rotation=0, labelsize=TICK_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_SIZE)
    ax.legend(title="Kommunikationsmodus", fontsize=LEGEND_SIZE, title_fontsize=LEGEND_SIZE)
    ax.set_ylim(0, max(percentage_df.max().max() * 1.18, 10))
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    add_bar_labels(ax, decimals=1, suffix="%")

    plt.tight_layout()
    plt.savefig(PHASE_OUTPUT, dpi=DPI, bbox_inches="tight")
    plt.close()


def plot_mode_by_account(account_df: pd.DataFrame) -> None:
    """
    Erstellt eine gestapelte Grafik der Kommunikationsmodi je Account.
    """
    pivot_df = account_df.pivot_table(
        index="account_name",
        columns="communication_mode",
        values="post_count",
        fill_value=0
    ).copy()

    ordered_columns = reorder_columns(pivot_df.columns.tolist())
    pivot_df = pivot_df[ordered_columns]

    pivot_df["total_posts"] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.sort_values("total_posts", ascending=False)
    pivot_df = pivot_df.drop(columns=["total_posts"])

    ax = pivot_df.plot(kind="bar", stacked=True, figsize=FIGSIZE_ACCOUNT)

    ax.set_title("Kommunikationsmodi je Account", fontsize=TITLE_SIZE, pad=12)
    ax.set_xlabel("Account", fontsize=LABEL_SIZE)
    ax.set_ylabel("Anzahl der Posts", fontsize=LABEL_SIZE)
    ax.tick_params(axis="x", rotation=45, labelsize=TICK_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_SIZE)
    plt.xticks(ha="right")
    ax.legend(title="Kommunikationsmodus", fontsize=LEGEND_SIZE, title_fontsize=LEGEND_SIZE)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    plt.tight_layout()
    plt.savefig(ACCOUNT_OUTPUT, dpi=DPI, bbox_inches="tight")
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