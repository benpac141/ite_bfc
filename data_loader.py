"""
data_loader.py — Chargement et préparation des données macrozone OPSAM.

Gère la lecture du CSV (séparateur décimal virgule), la dissolution du
shapefile par code macrozone, et le calcul des métriques agrégées.
Intègre les comptages PL issus de la matrice OD OPSAM.
"""

import os
import json
from functools import lru_cache
import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
import logging

log = logging.getLogger(__name__)

# Racine du package (dossier contenant data_loader.py)
_ROOT = Path(__file__).resolve().parent


def _path_env(ename: str, default: Path) -> Path:
    v = os.environ.get(ename)
    return Path(v) if v else default


def _data_local(name: str) -> Path:
    return _ROOT / "data" / name

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

CLASSES_DISTANCE = {
    "D1": "< 100 km",
    "D2": "100–200 km",
    "D3": "200–400 km",
    "D4": "400–1 000 km",
    "D5": "> 1 000 km",
}

TYPES_VOIE_LABELS = {
    "NoData": "Voies locales",
    "Departementale": "Voies départementales",
    "Nationale": "Voies nationales",
    "Autoroute": "Autoroutes",
}

FLUX_LABELS = {
    "E": "Échange",
    "T": "Transit",
    "I": "Interne",
}

# Chemins par defaut : priorite MACROZONE_* (env) > data/local/ > legacies reseau
_CSV_LOCAL = _data_local("macrozone_test_ITE.csv")
_CSV_LEGACY = Path(r"c:\Users\bpauc\Desktop\macrozone_test_ITE.csv")
CHEMIN_CSV_DEFAUT = _path_env("MACROZONE_CSV", _CSV_LOCAL if _CSV_LOCAL.exists() else _CSV_LEGACY)

_SHP_LOCAL = _data_local("opsam_zonage_metazone_ite_serm.shp")
_SHP_LEGACY = Path(
    r"U:\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\metazones_ITE_SERM\opsam_zonage_metazone_ite_serm.shp"
)
CHEMIN_SHP_DEFAUT = _path_env("MACROZONE_SHP", _SHP_LOCAL if _SHP_LOCAL.exists() else _SHP_LEGACY)
CHEMIN_NB_PL = Path(__file__).parent / "nb_pl_par_macrozone.csv"
CHEMIN_EMPLOI_FRET = Path(__file__).parent / "emploi_fret_par_macrozone.csv"
CHEMIN_EMPLOI_DETAIL = Path(__file__).parent / "emploi_fret_detail_par_macrozone.csv"
CHEMIN_ITE = Path(__file__).parent / "ite_par_macrozone.csv"
CHEMIN_ITE_DETAIL = Path(__file__).parent / "ite_detail_par_macrozone.csv"

_ITE_SHP_LOCAL = _data_local("ITE_BFC.shp")
_CHEMIN_SHP_ITE_NAS = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\ITE et plateformes\shp\ITE_BFC.shp"
)
CHEMIN_SHP_ITE = _path_env("MACROZONE_ITE_SHP", _ITE_SHP_LOCAL if _ITE_SHP_LOCAL.exists() else _CHEMIN_SHP_ITE_NAS)
CHEMIN_COURS_DETAIL = Path(__file__).parent / "cours_marchandise_detail.csv"


# ---------------------------------------------------------------------------
# Chargement CSV
# ---------------------------------------------------------------------------

def charger_csv_macrozone(chemin: str | Path = CHEMIN_CSV_DEFAUT) -> pd.DataFrame:
    """Lit le CSV macrozone et renvoie un DataFrame propre.

    Le fichier utilise la virgule comme séparateur décimal (locale FR).
    Les lignes M1 == 0 (zone « autre ») sont conservées mais signalées.
    """
    df = pd.read_csv(chemin, sep=",", decimal=",")
    df["M1"] = df["M1"].astype(int)
    df["CL_ADMIN_LABEL"] = df["CL_ADMIN"].map(TYPES_VOIE_LABELS).fillna(df["CL_ADMIN"])

    # Calcul VL déduit
    df["VKM_VL"] = df["VKM"] - df["VKM_PL"]

    # VKM VL par flux (E/T/I) déduits des totaux moins PL
    df["VKM_VL_E"] = df["VKM_E"] - (df["VKM_PL_EC"] + df["VKM_PL_EV"])
    df["VKM_VL_T"] = df["VKM_T"] - (df["VKM_PL_TC"] + df["VKM_PL_TV"])
    df["VKM_VL_I"] = df["VKM_I"] - (df["VKM_PL_IC"] + df["VKM_PL_IV"])

    # PL totaux par flux (chargés + vides)
    df["VKM_PL_E"] = df["VKM_PL_EC"] + df["VKM_PL_EV"]
    df["VKM_PL_T"] = df["VKM_PL_TC"] + df["VKM_PL_TV"]
    df["VKM_PL_I"] = df["VKM_PL_IC"] + df["VKM_PL_IV"]

    return df


# ---------------------------------------------------------------------------
# Chargement shapefile
# ---------------------------------------------------------------------------

def charger_shapefile_macrozone(
    chemin: str | Path = CHEMIN_SHP_DEFAUT,
) -> gpd.GeoDataFrame:
    """Charge le shapefile, dissout par MA_ITE et reprojette en WGS 84."""
    gdf = gpd.read_file(str(chemin))

    # Supprimer les zones sans macrozone (NaN)
    gdf = gdf.dropna(subset=["MA_ITE"])
    gdf["MA_ITE"] = gdf["MA_ITE"].astype(int)

    # Dissoudre par code macrozone
    gdf_dissolved = gdf.dissolve(by="MA_ITE", as_index=False)

    # Ne garder que les colonnes utiles
    gdf_dissolved = gdf_dissolved[["MA_ITE", "geometry"]]

    # Reprojeter en WGS 84 pour les cartes web
    gdf_dissolved = gdf_dissolved.to_crs(epsg=4326)

    return gdf_dissolved


# ---------------------------------------------------------------------------
# Calcul des métriques agrégées par macrozone
# ---------------------------------------------------------------------------

def calculer_metriques(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par macrozone et calcule les indicateurs clés.

    Renvoie un DataFrame avec une ligne par macrozone et les colonnes :
    VKM, VKM_VL, VKM_PL, VKM_E, VKM_T, VKM_I, DISTANCE,
    pct_transit, pct_echange, pct_interne, pct_pl, vkm_par_km, …
    """
    df_mz = df[df["M1"] != 0].copy()

    agg = (
        df_mz.groupby("M1")
        .agg(
            VKM=("VKM", "sum"),
            VKM_VL=("VKM_VL", "sum"),
            VKM_PL=("VKM_PL", "sum"),
            VKM_E=("VKM_E", "sum"),
            VKM_T=("VKM_T", "sum"),
            VKM_I=("VKM_I", "sum"),
            DISTANCE=("DISTANCE", "sum"),
            # Distance classes — échange VL+PL
            VKM_E_D1=("VKM_E_D1", "sum"),
            VKM_E_D2=("VKM_E_D2", "sum"),
            VKM_E_D3=("VKM_E_D3", "sum"),
            VKM_E_D4=("VKM_E_D4", "sum"),
            VKM_E_D5=("VKM_E_D5", "sum"),
            # Distance classes — transit VL+PL
            VKM_T_D1=("VKM_T_D1", "sum"),
            VKM_T_D2=("VKM_T_D2", "sum"),
            VKM_T_D3=("VKM_T_D3", "sum"),
            VKM_T_D4=("VKM_T_D4", "sum"),
            VKM_T_D5=("VKM_T_D5", "sum"),
            # Distance classes — interne VL+PL
            VKM_I_D1=("VKM_I_D1", "sum"),
            VKM_I_D2=("VKM_I_D2", "sum"),
            VKM_I_D3=("VKM_I_D3", "sum"),
            VKM_I_D4=("VKM_I_D4", "sum"),
            VKM_I_D5=("VKM_I_D5", "sum"),
            # PL chargés
            VKM_PL_EC=("VKM_PL_EC", "sum"),
            VKM_PL_TC=("VKM_PL_TC", "sum"),
            VKM_PL_IC=("VKM_PL_IC", "sum"),
            # PL vides
            VKM_PL_EV=("VKM_PL_EV", "sum"),
            VKM_PL_TV=("VKM_PL_TV", "sum"),
            VKM_PL_IV=("VKM_PL_IV", "sum"),
            # PL par distance — échange chargé
            VKM_PL_EC_D1=("VKM_PL_EC_D1", "sum"),
            VKM_PL_EC_D2=("VKM_PL_EC_D2", "sum"),
            VKM_PL_EC_D3=("VKM_PL_EC_D3", "sum"),
            VKM_PL_EC_D4=("VKM_PL_EC_D4", "sum"),
            VKM_PL_EC_D5=("VKM_PL_EC_D5", "sum"),
            # PL par distance — échange vide
            VKM_PL_EV_D1=("VKM_PL_EV_D1", "sum"),
            VKM_PL_EV_D2=("VKM_PL_EV_D2", "sum"),
            VKM_PL_EV_D3=("VKM_PL_EV_D3", "sum"),
            VKM_PL_EV_D4=("VKM_PL_EV_D4", "sum"),
            VKM_PL_EV_D5=("VKM_PL_EV_D5", "sum"),
            # PL par distance — transit chargé
            VKM_PL_TC_D1=("VKM_PL_TC_D1", "sum"),
            VKM_PL_TC_D2=("VKM_PL_TC_D2", "sum"),
            VKM_PL_TC_D3=("VKM_PL_TC_D3", "sum"),
            VKM_PL_TC_D4=("VKM_PL_TC_D4", "sum"),
            VKM_PL_TC_D5=("VKM_PL_TC_D5", "sum"),
            # PL par distance — transit vide
            VKM_PL_TV_D1=("VKM_PL_TV_D1", "sum"),
            VKM_PL_TV_D2=("VKM_PL_TV_D2", "sum"),
            VKM_PL_TV_D3=("VKM_PL_TV_D3", "sum"),
            VKM_PL_TV_D4=("VKM_PL_TV_D4", "sum"),
            VKM_PL_TV_D5=("VKM_PL_TV_D5", "sum"),
        )
        .reset_index()
    )

    # Pourcentages tous vehicules (TV)
    agg["pct_transit"] = (agg["VKM_T"] / agg["VKM"] * 100).round(1)
    agg["pct_echange"] = (agg["VKM_E"] / agg["VKM"] * 100).round(1)
    agg["pct_interne"] = (agg["VKM_I"] / agg["VKM"] * 100).round(1)
    agg["pct_pl"] = (agg["VKM_PL"] / agg["VKM"] * 100).round(1)

    # Pourcentages PL par flux
    vkm_pl_t = agg["VKM_PL_TC"] + agg["VKM_PL_TV"]
    vkm_pl_e = agg["VKM_PL_EC"] + agg["VKM_PL_EV"]
    vkm_pl_i = agg["VKM_PL_IC"] + agg["VKM_PL_IV"]
    agg["pct_transit_pl"] = (vkm_pl_t / agg["VKM_PL"].replace(0, np.nan) * 100).round(1)
    agg["pct_echange_pl"] = (vkm_pl_e / agg["VKM_PL"].replace(0, np.nan) * 100).round(1)
    agg["pct_interne_pl"] = (vkm_pl_i / agg["VKM_PL"].replace(0, np.nan) * 100).round(1)

    # VKM par km d'infrastructure
    agg["vkm_par_km"] = (agg["VKM"] / agg["DISTANCE"].replace(0, np.nan)).round(0)

    # Part longue distance (D4 + D5) — tous vehicules
    total_dist = (
        agg["VKM_E_D1"] + agg["VKM_E_D2"] + agg["VKM_E_D3"] + agg["VKM_E_D4"] + agg["VKM_E_D5"]
        + agg["VKM_T_D1"] + agg["VKM_T_D2"] + agg["VKM_T_D3"] + agg["VKM_T_D4"] + agg["VKM_T_D5"]
        + agg["VKM_I_D1"] + agg["VKM_I_D2"] + agg["VKM_I_D3"] + agg["VKM_I_D4"] + agg["VKM_I_D5"]
    )
    longue_dist = (
        agg["VKM_E_D4"] + agg["VKM_E_D5"]
        + agg["VKM_T_D4"] + agg["VKM_T_D5"]
        + agg["VKM_I_D4"] + agg["VKM_I_D5"]
    )
    agg["pct_longue_distance"] = (longue_dist / total_dist.replace(0, np.nan) * 100).round(1)

    # Part longue distance PL (D4 + D5) — poids lourds uniquement
    total_pl_dist = pd.Series(0.0, index=agg.index)
    longue_pl_dist = pd.Series(0.0, index=agg.index)
    for flux in ["E", "T", "I"]:
        for charge in ["C", "V"]:
            for d in ["D1", "D2", "D3", "D4", "D5"]:
                col = f"VKM_PL_{flux}{charge}_{d}"
                if col in agg.columns:
                    total_pl_dist += agg[col]
                    if d in ("D4", "D5"):
                        longue_pl_dist += agg[col]
    agg["pct_longue_distance_pl"] = (longue_pl_dist / total_pl_dist.replace(0, np.nan) * 100).round(1)

    # Convertir en milliers km/jour pour affichage
    agg["VKM_milliers"] = (agg["VKM"] / 1000).round(0)

    # Fusionner les comptages PL issus de la matrice OD
    agg = _fusionner_nb_pl(agg)

    # Fusionner les donnees d'enrichissement (emploi fret + ITE)
    agg = _fusionner_enrichissement(agg)

    return agg


def _fusionner_nb_pl(agg: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les colonnes nb_pl_* depuis le fichier pré-calculé."""
    if not CHEMIN_NB_PL.exists():
        log.warning("Fichier %s introuvable — colonnes nb_pl non ajoutées", CHEMIN_NB_PL)
        agg["nb_pl_echange"] = np.nan
        agg["nb_pl_interne"] = np.nan
        agg["nb_pl_total"] = np.nan
        return agg

    df_pl = pd.read_csv(CHEMIN_NB_PL)
    df_pl = df_pl.rename(columns={"code_macrozone": "M1"})
    cols_pl = ["M1", "nb_pl_echange", "nb_pl_interne", "nb_pl_total"]
    cols_present = [c for c in cols_pl if c in df_pl.columns]
    agg = agg.merge(df_pl[cols_present], on="M1", how="left")
    return agg


def _fusionner_enrichissement(agg: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les colonnes emploi fret et ITE depuis les fichiers pre-calcules."""
    if CHEMIN_EMPLOI_FRET.exists():
        df_emp = pd.read_csv(CHEMIN_EMPLOI_FRET)
        agg = agg.merge(df_emp, on="M1", how="left")
        agg["emploi_fret"] = agg["emploi_fret"].fillna(0).astype(int)
        agg["nb_etab_fret"] = agg["nb_etab_fret"].fillna(0).astype(int)
    else:
        log.warning("Fichier emploi fret introuvable: %s", CHEMIN_EMPLOI_FRET)
        agg["emploi_fret"] = 0
        agg["nb_etab_fret"] = 0

    if CHEMIN_ITE.exists():
        df_ite = pd.read_csv(CHEMIN_ITE)
        agg = agg.merge(df_ite[["M1", "nb_ite"]], on="M1", how="left")
        agg["nb_ite"] = agg["nb_ite"].fillna(0).astype(int)
    else:
        log.warning("Fichier ITE introuvable: %s", CHEMIN_ITE)
        agg["nb_ite"] = 0

    return agg


def charger_emploi_detail() -> pd.DataFrame:
    """Charge le detail emploi fret par secteur et macrozone."""
    if not CHEMIN_EMPLOI_DETAIL.exists():
        return pd.DataFrame(columns=["M1", "label_secteur", "emploi", "nb_etab"])
    return pd.read_csv(CHEMIN_EMPLOI_DETAIL)


def charger_ite_detail() -> pd.DataFrame:
    """Charge le detail des ITE avec leur macrozone d'appartenance."""
    if not CHEMIN_ITE_DETAIL.exists():
        return pd.DataFrame(columns=["id", "departement", "commune", "raison_sociale", "statut", "M1"])
    return pd.read_csv(CHEMIN_ITE_DETAIL)


def charger_ite_geodata() -> pd.DataFrame | None:
    """Charge les ITE avec lat/lon depuis le CSV pre-calcule."""
    if not CHEMIN_ITE_DETAIL.exists():
        return None
    try:
        df = pd.read_csv(CHEMIN_ITE_DETAIL)
        df = df.dropna(subset=["lat", "lon"])
        return df
    except Exception:
        return None


def charger_cours_marchandise() -> pd.DataFrame | None:
    """Charge les cours de marchandise avec lat/lon depuis le CSV pre-calcule."""
    if not CHEMIN_COURS_DETAIL.exists():
        return None
    try:
        df = pd.read_csv(CHEMIN_COURS_DETAIL)
        df = df.dropna(subset=["lat", "lon"])
        return df
    except Exception:
        return None


def calculer_score_composite(
    metriques: pd.DataFrame,
    seuil_distance: str = "D3",
    poids_trafic: float = 0.50,
    poids_emploi: float = 0.30,
    poids_ite: float = 0.20,
) -> pd.DataFrame:
    """Calcule un score composite de priorite pour le report modal.

    Combine trois dimensions normalisees [0-100] avec poids parametrables :
    - Trafic PL reportable (VKM PL longue distance >= seuil)
    - Emploi lie au fret (nb emplois secteurs generateurs)
    - Infrastructures terminales (nb ITE)
    """
    df = metriques.copy()

    # VKM PL reportable = somme des classes de distance >= seuil
    dist_order = list(CLASSES_DISTANCE.keys())
    idx_seuil = dist_order.index(seuil_distance)
    classes_retenues = dist_order[idx_seuil:]

    vkm_report = pd.Series(0.0, index=df.index)
    for flux in ["E", "T"]:
        for dc in classes_retenues:
            for charge in ["C", "V"]:
                col = f"VKM_PL_{flux}{charge}_{dc}"
                if col in df.columns:
                    vkm_report += df[col]
    df["vkm_pl_reportable"] = vkm_report

    def _norm(s):
        mn, mx = s.min(), s.max()
        return ((s - mn) / (mx - mn) * 100).round(1) if mx > mn else pd.Series(50, index=s.index)

    df["score_trafic"] = _norm(df["vkm_pl_reportable"])
    df["score_emploi"] = _norm(df["emploi_fret"])
    df["score_ite"] = _norm(df["nb_ite"])

    total_poids = poids_trafic + poids_emploi + poids_ite
    w_t = poids_trafic / total_poids
    w_e = poids_emploi / total_poids
    w_i = poids_ite / total_poids

    df["score_composite"] = (
        df["score_trafic"] * w_t
        + df["score_emploi"] * w_e
        + df["score_ite"] * w_i
    ).round(1)

    return df


# ---------------------------------------------------------------------------
# Préparation Sankey par macrozone
# ---------------------------------------------------------------------------

def preparer_donnees_sankey_macrozone(
    df: pd.DataFrame, code_mz: int
) -> pd.DataFrame:
    """Prépare les flux source → target → value pour un Sankey à 2 niveaux.

    Niveau 1 : Trafic total → Type de voie
    Niveau 2 : Type de voie → Interne / Échange / Transit
    """
    df_mz = df[df["M1"] == code_mz].copy()
    df_mz["CL_ADMIN_LABEL"] = df_mz["CL_ADMIN"].map(TYPES_VOIE_LABELS).fillna(df_mz["CL_ADMIN"])

    rows = []

    # Niveau 1 : Trafic total → type de voie
    for _, row in df_mz.iterrows():
        label_voie = row["CL_ADMIN_LABEL"]
        rows.append({"source": "Trafic total", "target": label_voie, "values": row["VKM"] / 1000})

    # Niveau 2 : type de voie → Interne / Échange / Transit
    for _, row in df_mz.iterrows():
        label_voie = row["CL_ADMIN_LABEL"]
        rows.append({"source": label_voie, "target": "Échange", "values": row["VKM_E"] / 1000})
        rows.append({"source": label_voie, "target": "Transit", "values": row["VKM_T"] / 1000})
        rows.append({"source": label_voie, "target": "Interne", "values": row["VKM_I"] / 1000})

    aggr = pd.DataFrame(rows)
    aggr = aggr.groupby(["source", "target"], as_index=False)["values"].sum()
    aggr = aggr[aggr["values"] > 0]

    return aggr


# ---------------------------------------------------------------------------
# Préparation données par classe de distance
# ---------------------------------------------------------------------------

def preparer_donnees_distance(
    df: pd.DataFrame, code_mz: int
) -> pd.DataFrame:
    """Extrait la répartition D1-D5 PL pour les 3 flux (E/T/I) d'une macrozone.

    Renvoie un DataFrame : flux | classe | vkm_milliers | label_classe
    Somme PL chargés + PL vides pour chaque flux/distance.
    """
    df_mz = df[df["M1"] == code_mz]

    records = []
    for flux_code, flux_label in FLUX_LABELS.items():
        for d_code, d_label in CLASSES_DISTANCE.items():
            val = 0
            for charge in ["C", "V"]:
                col = f"VKM_PL_{flux_code}{charge}_{d_code}"
                if col in df_mz.columns:
                    val += df_mz[col].sum()
            records.append({
                "flux": flux_label,
                "classe": d_code,
                "label_classe": d_label,
                "vkm_milliers": round(val / 1000, 1),
            })

    return pd.DataFrame(records)


def preparer_donnees_distance_pl(
    df: pd.DataFrame, code_mz: int
) -> pd.DataFrame:
    """Répartition D1-D5 pour les PL chargés et vides, par flux."""
    df_mz = df[df["M1"] == code_mz]

    records = []
    for flux_code, flux_label in FLUX_LABELS.items():
        for charge_label, suffixe in [("PL chargés", f"PL_{flux_code}C"), ("PL vides", f"PL_{flux_code}V")]:
            for d_code, d_label in CLASSES_DISTANCE.items():
                col = f"VKM_{suffixe}_{d_code}"
                val = df_mz[col].sum() / 1000 if col in df_mz.columns else 0
                records.append({
                    "flux": flux_label,
                    "type_pl": charge_label,
                    "classe": d_code,
                    "label_classe": d_label,
                    "vkm_milliers": round(val, 1),
                })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Répartition VL / PL
# ---------------------------------------------------------------------------

def calculer_repartition_vl_pl(df: pd.DataFrame, code_mz: int) -> dict:
    """Calcule la répartition VL/PL pour une macrozone donnée."""
    df_mz = df[df["M1"] == code_mz]
    total_vl = df_mz["VKM_VL"].sum()
    total_pl = df_mz["VKM_PL"].sum()
    total = total_vl + total_pl

    return {
        "vkm_vl": total_vl,
        "vkm_pl": total_pl,
        "total_vkm": total,
        "pct_vl": round(total_vl / total * 100, 1) if total > 0 else 0,
        "pct_pl": round(total_pl / total * 100, 1) if total > 0 else 0,
        # Détail PL chargés / vides
        "vkm_pl_charges": df_mz["VKM_PL_EC"].sum() + df_mz["VKM_PL_TC"].sum() + df_mz["VKM_PL_IC"].sum(),
        "vkm_pl_vides": df_mz["VKM_PL_EV"].sum() + df_mz["VKM_PL_TV"].sum() + df_mz["VKM_PL_IV"].sum(),
    }


# ---------------------------------------------------------------------------
# Labels macrozones à partir du shapefile
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4)
def _generer_labels_macrozones_mem(chemin_resolu: str) -> dict[int, str]:
    """Génère des libellés MA_ITE (communes phare + dép.) — résultat mis en cache par chemin."""
    try:
        gdf = gpd.read_file(chemin_resolu)
        gdf = gdf.dropna(subset=["MA_ITE"])
        gdf["MA_ITE"] = gdf["MA_ITE"].astype(int)

        labels = {}
        for mz in sorted(gdf["MA_ITE"].unique()):
            subset = gdf[gdf["MA_ITE"] == mz]
            deps = sorted(subset["INSEE_DEP"].dropna().unique())
            communes = (
                subset.sort_values("population", ascending=False)["NOM_COM"]
                .dropna()
                .head(2)
                .tolist()
            )
            dep_str = "/".join(str(d) for d in deps[:3])
            com_str = ", ".join(str(c) for c in communes)
            labels[mz] = f"MZ {mz} [{dep_str}] {com_str}"
        return labels
    except Exception:
        return {i: f"Macrozone {i}" for i in range(1, 37)}


def generer_labels_macrozones(chemin_shp: str | Path = CHEMIN_SHP_DEFAUT) -> dict[int, str]:
    """Génère des labels lisibles pour chaque macrozone à partir des communes du shapefile.

    Mise en cache par chemin de fichier (appels répétés du dashboard).
    """
    p = Path(chemin_shp).resolve()
    return _generer_labels_macrozones_mem(str(p))


# ---------------------------------------------------------------------------
# Analyse cordon Pagny
# ---------------------------------------------------------------------------

_CORDON_DEFAULT = _ROOT / "pagny_cordon_flows.csv"
CHEMIN_PAGNY_CORDON = _path_env("MACROZONE_PAGNY_CORDON_CSV", _CORDON_DEFAULT)
CHEMIN_PAGNY_VENT = _path_env("MACROZONE_PAGNY_VENT_JSON", _ROOT / "pagny_ventilation.json")
_ISO_LOCAL = _data_local("Pagny_aire_60min.geojson")
_ISO_NAS = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL"
    r"\2025-2026_MISSION_DREAL\2_INPUT\DATA\PAGNY_REPORT"
    r"\aire_60min_pagny\Pagny_aire_60min.geojson"
)
CHEMIN_PAGNY_ISOCHRONE = _path_env(
    "MACROZONE_PAGNY_ISO_GEOJSON",
    _ISO_LOCAL if _ISO_LOCAL.exists() else _ISO_NAS,
)

# Aire de chalandise 60 min — ports de Chalon-sur-Saone et Macon (GeoJSON)
_AIRES_CM_U = Path(
    r"U:\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\PAGNY_REPORT\aire_60min_chalon_macon"
)
_AIRES_CM_NAS = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\PAGNY_REPORT\aire_60min_chalon_macon"
)
# Par défaut : U: s’il existe, sinon miroir NAS. Surchargé par MACROZONE_AIRES_60M_CHALON_MACON_DIR
CHEMIN_AIRES_60M_CHALON_MACON = _path_env(
    "MACROZONE_AIRES_60M_CHALON_MACON_DIR",
    _AIRES_CM_U if _AIRES_CM_U.is_dir() else _AIRES_CM_NAS,
)


def _normaliser_colonne_cordon(df: pd.DataFrame) -> pd.DataFrame:
    """Si le CSV d'ancienne génération n'a pas `cordon`, on suppose Pagny seul."""
    if df.empty:
        return df
    if "cordon" not in df.columns:
        df = df.copy()
        df["cordon"] = "pagny"
    return df


def charger_cordon_pagny(chemin: str | Path | None = None) -> pd.DataFrame:
    """Charge le CSV pre-calcule des flux cordon (Pagny, eventuellement chalon, macon)."""
    p = Path(chemin) if chemin else CHEMIN_PAGNY_CORDON
    if not p.exists():
        log.warning("Fichier %s introuvable", p)
        return pd.DataFrame()
    return _normaliser_colonne_cordon(pd.read_csv(p))


def _enrichir_bassins_ventilation(d: dict) -> dict:
    """Construit ou complete ``bassins_ventilation`` (retrocompat JSON Fos/Sete seul)."""
    _MERGE_IDS = {
        "drome_valence_montelimar": {
            "id": "drome_valence_montelimar",
            "nom": "Drome (26) — Valence / Montélimar (agreg. OPSAM 5135)",
            "zone_opsam": 5135,
            "coeff": float(
                (d.get("drome_valence_montelimar") or {}).get("coeff", 0) or 0
            ),
            "source": "urssaf",
            "color": "#00695C",
        },
        "isere_vienne": {
            "id": "isere_vienne",
            "nom": "Isere (38) — port de Vienne (agreg. OPSAM 5235)",
            "zone_opsam": 5235,
            "coeff": float(
                (d.get("isere_vienne") or {}).get("coeff", 0) or 0
            ),
            "source": "urssaf",
            "color": "#5E35B1",
        },
        "chalon": {
            "id": "chalon",
            "nom": "Saone-et-Loire (71) — bassin Chalon (agreg. OPSAM dans JSON)",
            "zone_opsam": int((d.get("chalon") or {}).get("zone_opsam", 0) or 0),
            "coeff": float((d.get("chalon") or {}).get("coeff", 0) or 0),
            "source": "urssaf",
            "color": "#2E7D32",
        },
        "macon": {
            "id": "macon",
            "nom": "Saone-et-Loire (71) — bassin Macon (agreg. OPSAM dans JSON)",
            "zone_opsam": int((d.get("macon") or {}).get("zone_opsam", 0) or 0),
            "coeff": float((d.get("macon") or {}).get("coeff", 0) or 0),
            "source": "urssaf",
            "color": "#6A1B9A",
        },
    }

    cur = d.get("bassins_ventilation")
    if not cur:
        fos, sete = d.get("fos", {}), d.get("sete", {})
        out = [
            {
                "id": "fos",
                "nom": fos.get("nom", "ZIP Fos-Etang de Berre"),
                "zone_opsam": 5125,
                "coeff": float(fos.get("coeff", 0.1)),
                "source": "urssaf",
                "color": "#1565C0",
            },
            {
                "id": "sete",
                "nom": sete.get("nom", "Bassin Sete-Thau"),
                "zone_opsam": 5124,
                "coeff": float(sete.get("coeff", 0.05)),
                "source": "urssaf",
                "color": "#F57C00",
            },
        ]
        dvm = d.get("drome_valence_montelimar")
        isv = d.get("isere_vienne")
        if isinstance(dvm, dict) and dvm:
            out.append(
                {
                    "id": "drome_valence_montelimar",
                    "nom": dvm.get("nom", _MERGE_IDS["drome_valence_montelimar"]["nom"]),
                    "zone_opsam": int(dvm.get("zone_opsam", 5135)),
                    "coeff": float(dvm.get("coeff", 0)),
                    "source": dvm.get("source", "urssaf"),
                    "color": "#00695C",
                }
            )
        else:
            out.append(_MERGE_IDS["drome_valence_montelimar"])
        if isinstance(isv, dict) and isv:
            out.append(
                {
                    "id": "isere_vienne",
                    "nom": isv.get("nom", _MERGE_IDS["isere_vienne"]["nom"]),
                    "zone_opsam": int(isv.get("zone_opsam", 5235)),
                    "coeff": float(isv.get("coeff", 0)),
                    "source": isv.get("source", "urssaf"),
                    "color": "#5E35B1",
                }
            )
        else:
            out.append(_MERGE_IDS["isere_vienne"])
        c71 = d.get("chalon")
        m71 = d.get("macon")
        if isinstance(c71, dict) and c71:
            out.append(
                {
                    "id": "chalon",
                    "nom": c71.get("nom", _MERGE_IDS["chalon"]["nom"]),
                    "zone_opsam": int(c71.get("zone_opsam", 0) or 0),
                    "coeff": float(c71.get("coeff", 0) or 0),
                    "source": c71.get("source", "urssaf"),
                    "color": "#2E7D32",
                }
            )
        else:
            out.append(_MERGE_IDS["chalon"])
        if isinstance(m71, dict) and m71:
            out.append(
                {
                    "id": "macon",
                    "nom": m71.get("nom", _MERGE_IDS["macon"]["nom"]),
                    "zone_opsam": int(m71.get("zone_opsam", 0) or 0),
                    "coeff": float(m71.get("coeff", 0) or 0),
                    "source": m71.get("source", "urssaf"),
                    "color": "#6A1B9A",
                }
            )
        else:
            out.append(_MERGE_IDS["macon"])
        out.append(
            {
                "id": "pagny_hinterland",
                "nom": "Influence Aproport / Chalon–Macon–Pagny (optionnel, ID a renseigner)",
                "zone_opsam": 0,
                "coeff": 0.0,
                "source": "manuel",
                "color": "#00838F",
            }
        )
        d["bassins_ventilation"] = out
        return d

    have = {b.get("id") for b in cur if isinstance(b, dict)}
    for mid, blk in _MERGE_IDS.items():
        if mid not in have:
            c_from = d.get(mid) if isinstance(d.get(mid), dict) else {}
            c_src = c_from if isinstance(c_from, dict) else {}
            v = c_src.get("coeff", None)
            coeff = float(blk["coeff"] if v is None else v)
            to_add = {**blk, "coeff": coeff}
            d["bassins_ventilation"] = list(cur) + [to_add]
            cur = d["bassins_ventilation"]
            have.add(mid)
    return d


def charger_ventilation_fos_sete() -> dict:
    """Charge le JSON de ventilation (Fos/Sete + `bassins_ventilation` etendu)."""
    if not CHEMIN_PAGNY_VENT.exists():
        log.warning("Fichier %s introuvable", CHEMIN_PAGNY_VENT)
        return {
            "fos": {"coeff": 0.10, "nom": "ZIP Fos-Etang de Berre",
                    "region_label": "PACA", "emploi_bassin": 0, "emploi_region": 0,
                    "communes": {}, "detail_communes": {}},
            "sete": {"coeff": 0.05, "nom": "Bassin portuaire Sete-Thau",
                     "region_label": "Occitanie", "emploi_bassin": 0,
                     "emploi_region": 0, "communes": {}, "detail_communes": {}},
            "corridor": {"coeff_fos": 0.10, "coeff_sete": 0.05},
            "bassins_ventilation": _enrichir_bassins_ventilation(
                {"fos": {"coeff": 0.10, "nom": "ZIP Fos"},
                 "sete": {"coeff": 0.05, "nom": "Sete"}}
            )["bassins_ventilation"],
        }
    with open(CHEMIN_PAGNY_VENT, "r", encoding="utf-8") as f:
        data = json.load(f)
    return _enrichir_bassins_ventilation(data)


def charger_isochrone_pagny():
    """Charge le GeoJSON de l'isochrone 1h Pagny et reprojette en WGS84."""
    if not CHEMIN_PAGNY_ISOCHRONE.exists():
        log.warning("Fichier %s introuvable", CHEMIN_PAGNY_ISOCHRONE)
        return None
    gdf = gpd.read_file(str(CHEMIN_PAGNY_ISOCHRONE))
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def _gdf_wgs84(gdf):
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def _libelle_et_couleurs_chalon_macon(path: Path) -> tuple[str, str, str]:
    """(légende, remplissage rgba, couleur contour) d’après le nom de fichier."""
    s = path.stem.lower()
    if "chalon" in s:
        return "Aire 1h chalandise — Chalon", "rgba(46,125,50,0.18)", "#2E7D32"
    if "macon" in s or "mâcon" in path.stem.lower():
        return "Aire 1h chalandise — Mâcon", "rgba(106,27,154,0.18)", "#6A1B9A"
    return f"Aire 1h — {path.stem}", "rgba(0,131,143,0.16)", "#00838F"


def _fichiers_geometrie_aire_port(d: Path) -> list[Path]:
    """Liste .geojson et .json (GeoJSON typique) dans le dossier."""
    files = list(sorted(d.glob("*.geojson")))
    for p in sorted(d.glob("*.json")):
        if p not in files:
            try:
                head = p.read_text(encoding="utf-8", errors="ignore")[:200]
            except OSError:
                continue
            if "Feature" in head or "geometry" in head or "features" in head:
                files.append(p)
    return files


def signature_fichiers_aires_chalon_macon() -> str:
    """Cles pour invalidation du cache Streamlit (mtimes + noms)."""
    d = CHEMIN_AIRES_60M_CHALON_MACON
    if not d.is_dir():
        return f"nodir:{d}"
    parts = []
    for p in _fichiers_geometrie_aire_port(d):
        try:
            parts.append(f"{p.name}:{p.stat().st_mtime}")
        except OSError:
            parts.append(p.name)
    return f"{d}|" + "|".join(parts)


def charger_aires_60min_chalon_macon():
    """Charge tous les GeoJSON du dossier aire_60min_chalon_macon (carte / contexte).

    Chaque élément : ``gdf``, ``label``, ``fill``, ``line``, ``path``,
    ``centroid`` (lat, lon) pour marqueur.
    """
    d = CHEMIN_AIRES_60M_CHALON_MACON
    if not d.is_dir():
        log.debug("Dossier aires 60m Chalon/Mâcon introuvable: %s", d)
        return []

    out = []
    for p in _fichiers_geometrie_aire_port(d):
        try:
            gdf = gpd.read_file(str(p))
            gdf = _gdf_wgs84(gdf)
        except Exception as e:
            log.warning("Lecture %s: %s", p, e)
            continue
        if gdf.empty or gdf.geometry.isna().all():
            continue
        label, fill, line = _libelle_et_couleurs_chalon_macon(p)
        try:
            u = gdf.geometry.union_all() if hasattr(gdf.geometry, "union_all") else gdf.unary_union
            c = u.centroid
            lat, lon = float(c.y), float(c.x)
        except Exception:
            try:
                u = gdf.unary_union
                c = u.centroid
                lat, lon = float(c.y), float(c.x)
            except Exception:
                lat, lon = 46.78, 4.85
        out.append(
            {
                "path": p,
                "label": label,
                "gdf": gdf,
                "fill": fill,
                "line": line,
                "centroid": (lat, lon),
            }
        )
    return out
