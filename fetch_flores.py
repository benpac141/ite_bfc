"""
fetch_flores.py - Telecharge les donnees emploi/etablissements depuis l'API URSSAF
et agrege par macrozone pour les secteurs generateurs de fret.
"""

import requests
import pandas as pd
import io
from pathlib import Path
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

LOOKUP_PATH = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\ITE et plateformes\lookup_ opsam_ite\lookup_dep_com_epci_macrozone.csv"
)
OUTPUT_DIR = Path(__file__).parent

EXPORT_URL = (
    "https://open.urssaf.fr/api/explore/v2.1/catalog/datasets/"
    "etablissements-et-effectifs-salaries-au-niveau-commune-x-ape-last/exports/csv"
)

DEPS_BFC = ["21", "25", "39", "58", "70", "71", "89", "90"]

SECTEURS_FRET_NA88 = {
    "01": "Culture et elevage",
    "02": "Sylviculture",
    "03": "Peche / aquaculture",
    "08": "Industries extractives",
    "10": "Ind. alimentaires",
    "11": "Fabrication boissons",
    "16": "Travail bois",
    "17": "Industrie papier/carton",
    "20": "Industrie chimique",
    "22": "Caoutchouc/plastique",
    "23": "Produits mineraux",
    "24": "Metallurgie",
    "25": "Produits metalliques",
    "28": "Machines/equipements",
    "29": "Industrie automobile",
    "30": "Autres materiels transport",
    "41": "Construction batiments",
    "42": "Genie civil",
    "43": "Travaux construction spec.",
    "46": "Commerce gros",
    "49": "Transports terrestres",
    "52": "Entreposage / services transport",
}


def _extraire_code_na88(val):
    if pd.isna(val):
        return None
    parts = str(val).strip().split(" ", 1)
    return parts[0] if parts else None


def telecharger_emploi_bfc():
    """Telecharge le CSV complet des emplois BFC depuis l'API URSSAF."""
    dep_filter = " OR ".join(f'code_departement="{d}"' for d in DEPS_BFC)

    params = {
        "where": dep_filter,
        "select": (
            "code_commune,commune,code_departement,secteur_na88,"
            "effectifs_salaries_2023,effectifs_salaries_2022,"
            "nombre_d_etablissements_2023,nombre_d_etablissements_2022"
        ),
        "delimiter": ";",
        "limit": -1,
    }

    print("Telechargement CSV depuis open.urssaf.fr...", flush=True)
    r = requests.get(EXPORT_URL, params=params, timeout=300)
    r.raise_for_status()
    print(f"  Recu {len(r.content) / 1024 / 1024:.1f} MB", flush=True)

    df = pd.read_csv(io.StringIO(r.text), sep=";")
    print(f"  {len(df)} enregistrements, colonnes: {list(df.columns)}", flush=True)
    return df


def agreger_par_macrozone(df):
    """Agrege les emplois par macrozone pour les secteurs generateurs de fret."""
    lookup = pd.read_csv(LOOKUP_PATH, sep=None, engine="python")
    lookup["COM"] = lookup["COM"].astype(str).str.zfill(5)

    df["code_commune"] = df["code_commune"].astype(str).str.zfill(5)
    df["code_na88"] = df["secteur_na88"].apply(_extraire_code_na88)

    year_col = "effectifs_salaries_2023"
    etab_col = "nombre_d_etablissements_2023"
    if year_col not in df.columns or df[year_col].isna().all():
        year_col = "effectifs_salaries_2022"
        etab_col = "nombre_d_etablissements_2022"

    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").fillna(0)
    df[etab_col] = pd.to_numeric(df[etab_col], errors="coerce").fillna(0)

    df_fret = df[df["code_na88"].isin(SECTEURS_FRET_NA88.keys())].copy()
    df_fret["label_secteur"] = df_fret["code_na88"].map(SECTEURS_FRET_NA88)
    print(f"  Enregistrements fret: {len(df_fret)}", flush=True)

    df_merged = df_fret.merge(
        lookup[["COM", "M1"]],
        left_on="code_commune",
        right_on="COM",
        how="inner",
    )
    df_merged = df_merged[df_merged["M1"] != 0]
    print(f"  Apres jointure macrozone: {len(df_merged)}", flush=True)

    emploi_total = (
        df_merged.groupby("M1")
        .agg(emploi_fret=(year_col, "sum"), nb_etab_fret=(etab_col, "sum"))
        .reset_index()
    )
    emploi_total["emploi_fret"] = emploi_total["emploi_fret"].astype(int)
    emploi_total["nb_etab_fret"] = emploi_total["nb_etab_fret"].astype(int)

    out1 = OUTPUT_DIR / "emploi_fret_par_macrozone.csv"
    emploi_total.to_csv(out1, index=False)
    print(f"\nEmploi fret par macrozone ({out1}):", flush=True)
    print(
        emploi_total.sort_values("emploi_fret", ascending=False).head(10).to_string(index=False),
        flush=True,
    )

    emploi_detail = (
        df_merged.groupby(["M1", "label_secteur"])
        .agg(emploi=(year_col, "sum"), nb_etab=(etab_col, "sum"))
        .reset_index()
    )
    emploi_detail["emploi"] = emploi_detail["emploi"].astype(int)
    emploi_detail["nb_etab"] = emploi_detail["nb_etab"].astype(int)

    out2 = OUTPUT_DIR / "emploi_fret_detail_par_macrozone.csv"
    emploi_detail.to_csv(out2, index=False)
    print(f"Detail exporte: {out2}", flush=True)

    return emploi_total, emploi_detail


if __name__ == "__main__":
    df = telecharger_emploi_bfc()
    agreger_par_macrozone(df)
    print("\nTermine.", flush=True)
