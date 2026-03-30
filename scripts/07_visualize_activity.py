"""
07_visualize_activity.py

Erstellt Visualisierungen auf Basis der Aktivitätsanalyse.

Ziele:
- Posting-Aktivität über die Zeit darstellen
- Aktivität je Account zwischen Phase A und Phase B vergleichen
- Gesamtaktivität je Account visualisieren

Eingabedateien:
- data/processed/activity_by_week.csv
- data/processed/activity_by_phase.csv
- data/processed/activity_total.csv

Ausgabedateien:
- figures/activity_over_time.png
- figures/activity_by_phase.png
- figures/activity_total.png
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


INPUT_DIR = Path("data") / "processed"
OUTPUT_DIR = Path("figures")

WEEKLY_ACTIVITY_PATH = INPUT_DIR / "activity_by_week.csv"
PHASE_ACTIVITY_PATH = INPUT_DIR / "activity_by_phase.csv"
TOTAL_ACTIVITY_PATH = INPUT_DIR / "activity_total.csv"

OUTPUT_OVER_TIME = OUTPUT_DIR / "activity_over_time.png"
OUTPUT_BY_PHASE = OUTPUT_DIR / "activity_by_phase.png"
OUTPUT_TOTAL = OUTPUT_DIR / "activity_total.png"


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Zielordner für Grafiken existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lädt die vorbereiteten Analyse-CSV-Dateien.
    """
    weekly_df = pd.read_csv(WEEKLY_ACTIVITY_PATH)
    phase_df = pd.read_csv(PHASE_ACTIVITY_PATH)
    total_df = pd.read_csv(TOTAL_ACTIVITY_PATH)

    return weekly_df, phase_df, total_df


def sort_year_week_labels(labels: list[str]) -> list[str]:
    """
    Sortiert Labels im Format YYYY-WWW chronologisch.
    """

    def sort_key(label: str) -> tuple[int, int]:
        year_part, week_part = label.split("-W")
        return int(year_part), int(week_part)

    return sorted(labels, key=sort_key)


def plot_activity_over_time(weekly_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik der gesamten Posting-Aktivität pro Woche.
    """
    aggregated = (
        weekly_df.groupby("year_week", as_index=False)
        .agg(post_count=("post_count", "sum"))
        .copy()
    )

    ordered_labels = sort_year_week_labels(aggregated["year_week"].tolist())
    aggregated["year_week"] = pd.Categorical(
        aggregated["year_week"],
        categories=ordered_labels,
        ordered=True
    )
    aggregated = aggregated.sort_values("year_week")

    plt.figure(figsize=(12, 6))
    plt.plot(aggregated["year_week"].astype(str), aggregated["post_count"], marker="o")
    plt.title("Posting-Aktivität pro Woche")
    plt.xlabel("Kalenderwoche")
    plt.ylabel("Anzahl der Posts")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(OUTPUT_OVER_TIME, dpi=300, bbox_inches="tight")
    plt.close()


def plot_activity_by_phase(phase_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik zum Vergleich der Aktivität je Account
    zwischen Phase A und Phase B.
    """
    pivot_df = phase_df.pivot_table(
        index="account_name",
        columns="phase",
        values="post_count",
        fill_value=0
    ).copy()

    for phase_name in ["Phase A", "Phase B"]:
        if phase_name not in pivot_df.columns:
            pivot_df[phase_name] = 0

    pivot_df = pivot_df[["Phase A", "Phase B"]]
    pivot_df = pivot_df.sort_values("Phase B", ascending=False)

    ax = pivot_df.plot(kind="bar", figsize=(12, 7))
    ax.set_title("Posting-Aktivität je Account nach Phase")
    ax.set_xlabel("Account")
    ax.set_ylabel("Anzahl der Posts")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(OUTPUT_BY_PHASE, dpi=300, bbox_inches="tight")
    plt.close()


def plot_total_activity(total_df: pd.DataFrame) -> None:
    """
    Erstellt eine Grafik der Gesamtaktivität je Account
    im gesamten Untersuchungszeitraum.
    """
    plot_df = total_df.sort_values("total_posts", ascending=False).copy()

    plt.figure(figsize=(12, 7))
    plt.bar(plot_df["account_name"], plot_df["total_posts"])
    plt.title("Gesamtaktivität je Account")
    plt.xlabel("Account")
    plt.ylabel("Anzahl der Posts")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(OUTPUT_TOTAL, dpi=300, bbox_inches="tight")
    plt.close()


def main() -> None:
    """
    Führt die Visualisierung der Aktivitätsdaten vollständig aus.
    """
    ensure_output_directory()

    print("Lade Analyse-Dateien ...")
    weekly_df, phase_df, total_df = load_data()

    print("Erstelle Grafik: Aktivität über die Zeit ...")
    plot_activity_over_time(weekly_df)

    print("Erstelle Grafik: Aktivität nach Phase ...")
    plot_activity_by_phase(phase_df)

    print("Erstelle Grafik: Gesamtaktivität je Account ...")
    plot_total_activity(total_df)

    print("Visualisierung abgeschlossen.")
    print(f"- {OUTPUT_OVER_TIME}")
    print(f"- {OUTPUT_BY_PHASE}")
    print(f"- {OUTPUT_TOTAL}")


if __name__ == "__main__":
    main()