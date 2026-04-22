"""
prepare_pagny.py - Pre-calcul de l'analyse cordon Pagny.

Lit les 6 matrices PL (EC, EV, IC, IV, TC, TV), filtre les flux
traversant le cordon de l'isochrone 1h autour de Pagny, ajoute les
distances et les metadonnees de zone, et exporte un CSV agrege.

Calcule egalement les coefficients de ventilation Fos/Sete via
l'emploi fret URSSAF.
"""

import os
import sys
import json
import io
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Taille des blocs pour lire les matrices (reduit la charge RAM). 0 = lecture monolithique (ancien mode).
CHUNK_LIGNES = int(os.environ.get("MACROZONE_MATRIX_CHUNK", "1000000"))

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------

def _data_opsam_root() -> Path:
    """Racine des matrices / distances (défaut NAS). Surcharge : `MACROZONE_DATA_ROOT`."""
    v = os.environ.get("MACROZONE_DATA_ROOT", "").strip()
    if v:
        return Path(v).expanduser()
    return Path(
        r"\\nas-bfc\COMMUN\21_MOBILITE\21.2_COMMUN\DATA"
    )


DATA_ROOT = _data_opsam_root()
MATRICE_DIR = DATA_ROOT / "MATRICE_FLUX"
_DIST_DIR = DATA_ROOT / "DISTANCE_MATRICE"
# ref2024 : nom actuel côté données ; ancien `matrice_distance_opsam.csv` en secours
# Surcharge : variable d’environnement `MACROZONE_DISTANCE_CSV` (chemin absolu ou relatif)
_DIST_CANDIDATES = (
    "matrice_distance_opsam_ref2024.csv",
    "matrice_distance_opsam.csv",
)


def matrice_distance_opsam_path() -> Path:
    """Chemin du CSV I,J,distance (V1) : env > ref2024 > fichier historique."""
    ovr = os.environ.get("MACROZONE_DISTANCE_CSV") or os.environ.get("PAGNY_DISTANCE_CSV")
    if ovr:
        return Path(ovr).expanduser()
    for name in _DIST_CANDIDATES:
        p = _DIST_DIR / name
        if p.is_file():
            return p
    return _DIST_DIR / _DIST_CANDIDATES[0]

def _dossier_pagny_report() -> Path:
    """Dossier PAGNY_REPORT : variable d’environnement, sinon U: s’il existe, sinon NAS."""
    env = os.environ.get("PAGNY_REPORT", "").strip()
    if env:
        return Path(env)
    u = Path(
        r"U:\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
        r"\2_INPUT\DATA\PAGNY_REPORT"
    )
    if u.is_dir():
        return u
    return Path(
        r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL"
        r"\2025-2026_MISSION_DREAL\2_INPUT\DATA\PAGNY_REPORT"
    )


PAGNY_REPORT = _dossier_pagny_report()
ZONES_EXCEL = PAGNY_REPORT / "liste_zones_pagny60min_distincation_paca.xlsx"
# Chalon / Mâcon : mêmes réf. DREAL que Pagny (*distincation_paca*). Repli : anciens noms `liste_zones_*_60min.xlsx`.
# Excel (prioritaire) ou CSV dans `data/`.
# Onglets attendus pour l’**agrégat** OPSAM (onglet *Bassins*), distinct de la 1re feuille listant souvent l’isochrone 1 h
SHEET_ZONE_AGREGE_CHALON = os.environ.get("BASSIN_VENTIL_SHEET_CHALON", "zone_chalon").strip() or "zone_chalon"
SHEET_ZONE_AGREGE_MACON = os.environ.get("BASSIN_VENTIL_SHEET_MACON", "zone_macon").strip() or "zone_macon"
_DATA_DIR = Path(__file__).parent / "data"
ZONES_CHALON_CSV = _DATA_DIR / "zones_chalon_60min.csv"
ZONES_MACON_CSV = _DATA_DIR / "zones_macon_60min.csv"


def _excel_zones_chalon() -> Path:
    """Fichier Excel des zones 1 h Chalon dans ``PAGNY_REPORT`` (réf. DREAL, puis ancien nom)."""
    cands = (
        PAGNY_REPORT / "liste_zones_chalon60min_distincation_paca.xlsx",
        PAGNY_REPORT / "liste_zones_chalon_60min.xlsx",
    )
    for p in cands:
        if p.is_file():
            return p
    return cands[0]


def _excel_zones_macon() -> Path:
    """Fichier Excel des zones 1 h Mâcon dans ``PAGNY_REPORT`` (réf. DREAL, puis ancien nom)."""
    cands = (
        PAGNY_REPORT / "liste_zones_macon60min_distincation_paca.xlsx",
        PAGNY_REPORT / "liste_zones_macon_60min.xlsx",
    )
    for p in cands:
        if p.is_file():
            return p
    return cands[0]


def _lookup_path() -> Path:
    """Lookup OPSAM. Surcharge : ``MACROZONE_LOOKUP_CSV`` (ex. bundle `data/bundle_publication/lookup/...`)."""
    v = os.environ.get("MACROZONE_LOOKUP_CSV", "").strip()
    if v:
        return Path(v).expanduser()
    return Path(
        r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
        r"\2_INPUT\DATA\ITE et plateformes\lookup_ opsam_ite"
        r"\lookup_dep_com_epci_macrozone.csv"
    )


LOOKUP_PATH = _lookup_path()

OUTPUT_DIR = Path(__file__).parent
# Sorties : par defaut racine du script ; ecrasement possible (ex. output_dev/) sans toucher a la baseline
CORDON_CSV_NAME = os.environ.get("PAGNY_CORDON_CSV", "pagny_cordon_flows.csv")
VENTILATION_JSON_NAME = os.environ.get("PAGNY_VENTILATION_JSON", "pagny_ventilation.json")

MATRICES_PL = {
    "EC": MATRICE_DIR / "MATRICE_PL_EC_Ref2024.csv",
    "EV": MATRICE_DIR / "MATRICE_PL_EV_Ref2024.CSV",
    "IC": MATRICE_DIR / "MATRICE_PL_IC_Ref2024.csv",
    "IV": MATRICE_DIR / "MATRICE_PL_IV_Ref2024.CSV",
    "TC": MATRICE_DIR / "MATRICE_PL_TC_Ref2024.CSV",
    "TV": MATRICE_DIR / "MATRICE_PL_TV_Ref2024.CSV",
}

SEUILS_DISTANCE = [
    (100, 200, "D2"),
    (200, 400, "D3"),
    (400, 1000, "D4"),
    (1000, float("inf"), "D5"),
]

URSSAF_EXPORT_URL = (
    "https://open.urssaf.fr/api/explore/v2.1/catalog/datasets/"
    "etablissements-et-effectifs-salaries-au-niveau-commune-x-ape-last/exports/csv"
)
SECTEURS_FRET = ["49", "52"]


# ---------------------------------------------------------------------------
# Chargement des zones Pagny
# ---------------------------------------------------------------------------

def charger_zones_pagny() -> set[int]:
    # Seules les lignes de cette feuille **modifient** l’agrégat « cordon = pagny »
    # (independant de Chalon / Mâcon, qui ont leurs propres listes + colonne `cordon` au CSV).
    if not ZONES_EXCEL.is_file():
        raise FileNotFoundError(
            f"Excel Pagny introuvable : {ZONES_EXCEL}\n"
            f"  (variable PAGNY_REPORT={PAGNY_REPORT})"
        )
    print(
        f"  Fichier Pagny : {ZONES_EXCEL}  —  feuille requise : 'zone_pagny'",
        flush=True,
    )
    df = pd.read_excel(ZONES_EXCEL, sheet_name="zone_pagny")
    zones = set(df.iloc[:, 0].dropna().astype(int).tolist())
    print(f"  Zones Pagny chargees : {len(zones)}", flush=True)
    return zones


def charger_zones_fichier(path: Path):
    """Charge un ensemble de codes zone OPSAM depuis .xlsx (1re feuille) ou .csv (1re colonne)."""
    if not path.exists():
        return None
    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path, header=0)
        else:
            df = pd.read_excel(path, sheet_name=0)
        s = df.iloc[:, 0].dropna()
        zones = {int(float(x)) for x in s if str(x).strip() not in ("", "nan")}
        if not zones:
            print(
                f"  {path.name} : fichier present mais 0 code zone (1re feuille / 1re colonne).",
                flush=True,
            )
        return zones if zones else None
    except Exception as e:
        print(f"  Lecture {path} impossible: {e}", flush=True)
        return None


def regler_cordons():
    """Liste des jeux (id cordon, zones) à calculer. Pagny obligatoire, Chalon/Mâcon si fichiers fournis."""
    res = [("pagny", charger_zones_pagny())]
    for cid, p_xls, p_csv in (
        ("chalon", _excel_zones_chalon(), ZONES_CHALON_CSV),
        ("macon", _excel_zones_macon(), ZONES_MACON_CSV),
    ):
        zs = charger_zones_fichier(p_xls) or charger_zones_fichier(p_csv)
        if zs and len(zs) > 0:
            which = p_xls if p_xls.exists() else p_csv
            print(f"Zones {cid}: {len(zs)} — {which}", flush=True)
            res.append((cid, zs))
        else:
            x_ok = f" oui — {p_xls}" if p_xls.is_file() else f" non — {p_xls.name}"
            c_ok = f" oui — {p_csv}" if p_csv.is_file() else f" non — {p_csv.name}"
            print(
                f"  Cordon « {cid} » : inactif (0 zone OPSAM dans la 1re feuille / colonne).",
                flush=True,
            )
            print(
                f"     Excel {p_xls.name} :{x_ok}  |  CSV repli {p_csv.name} :{c_ok}",
                flush=True,
            )
            print(
                f"     → voir { _DATA_DIR / 'README_CORDON.md' } (section analyse matricielle).",
                flush=True,
            )
    print(
        "\n  (Les totaux « pagny » ne changent qu’en editant la feuille 'zone_pagny' de "
        "l’Excel Pagny, pas seulement les listes Chalon / Mâcon.)\n",
        flush=True,
    )
    return res


def pipeline_un_cordon(zones: set[int], cordon_id: str) -> pd.DataFrame:
    """Filtre matrices, distances >100 km, enrichissement + colonne `cordon`."""
    print(f"\n--- Cordon : {cordon_id} — lecture matrices PL ---", flush=True)
    df = lire_et_filtrer_matrices(zones)
    if df.empty:
        return df
    df["cordon"] = cordon_id
    df = ajouter_distances(df)
    df = enrichir_zones(df)
    return df


# ---------------------------------------------------------------------------
# Lecture et filtrage des matrices PL
# ---------------------------------------------------------------------------

def _filtrer_cordon_une_matrice(
    df: pd.DataFrame, zones_pagny: set[int], code: str
) -> pd.DataFrame:
    """Filtre une tranche d'une matrice (I,J,V) vers le cordon d'une seule matrice code."""
    df = df[df["V"] > 0]
    if df.empty:
        return pd.DataFrame(columns=["I", "J", "V", "flux_type", "charge", "direction", "zone_ext"])

    i_in = df["I"].isin(zones_pagny)
    j_in = df["J"].isin(zones_pagny)
    cordon = df[i_in ^ j_in].copy()
    if cordon.empty:
        return cordon

    flux_type = code[0]  # E, I, T
    charge = code[1]  # C, V
    cordon["flux_type"] = flux_type
    cordon["charge"] = charge
    cordon["direction"] = "sortant"
    j_match = j_in.reindex(cordon.index).fillna(False)
    cordon.loc[j_match, "direction"] = "entrant"
    i_match = i_in.reindex(cordon.index).fillna(False)
    cordon["zone_ext"] = cordon["I"].where(~i_match, cordon["J"])
    return cordon


def lire_et_filtrer_matrices(zones_pagny: set[int]) -> pd.DataFrame:
    """Lit les 6 matrices PL, conserve les flux cordon et concatene.

    Par defaut, lecture par blocs (``MACROZONE_MATRIX_CHUNK``) pour reduire
    l'usage memoire sur les gros exports CSV. Mettre ``MACROZONE_MATRIX_CHUNK=0``
    pour revenir au mode lecture complete (meme resultat, plus de RAM).
    """
    _cols = ["I", "J", "V", "flux_type", "charge", "direction", "zone_ext"]
    frames = []
    for code, path in MATRICES_PL.items():
        print(f"  Lecture {code}: {path.name}...", end=" ", flush=True)
        if CHUNK_LIGNES and CHUNK_LIGNES > 0:
            parts = []
            n_chunks = 0
            for chunk in pd.read_csv(
                path,
                header=None,
                names=["I", "J", "V"],
                chunksize=CHUNK_LIGNES,
                engine="c",
            ):
                n_chunks += 1
                p = _filtrer_cordon_une_matrice(chunk, zones_pagny, code)
                if not p.empty:
                    parts.append(p)
            if parts:
                one = pd.concat(parts, ignore_index=True)
            else:
                one = pd.DataFrame(columns=_cols)
            print(
                f"{len(one)} flux cordon ({n_chunks} blocs, max {CHUNK_LIGNES} l./bloc)",
                flush=True,
            )
        else:
            df = pd.read_csv(path, header=None, names=["I", "J", "V"], engine="c")
            one = _filtrer_cordon_une_matrice(df, zones_pagny, code)
            print(f"{len(one)} flux cordon (fichier entier en RAM)", flush=True)
        if not one.empty:
            frames.append(one)

    if not frames:
        return pd.DataFrame(columns=_cols)
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Enrichissement distances
# ---------------------------------------------------------------------------

def ajouter_distances(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute la distance reseau et filtre > 100 km."""
    dist_path = matrice_distance_opsam_path()
    if not dist_path.is_file():
        raise FileNotFoundError(
            f"Matrice de distances introuvable: {dist_path}\n"
            f"  Attendu sous {_DIST_DIR} : {_DIST_CANDIDATES[0]} (ou {_DIST_CANDIDATES[1]}) ; "
            f"ou définir MACROZONE_DISTANCE_CSV=chemin_complet"
        )
    print(f"Chargement matrice de distances ({dist_path.name})...", flush=True)
    dist = pd.read_csv(dist_path, dtype={"I": int, "J": int, "V1": float})
    dist.columns = ["I", "J", "dist_km"]

    df = df.merge(dist, on=["I", "J"], how="left")
    avant = len(df)
    df = df[df["dist_km"] >= 100].copy()
    print(f"  Filtre >100km: {avant} -> {len(df)} flux", flush=True)

    def classer(d):
        for lo, hi, label in SEUILS_DISTANCE:
            if lo <= d < hi:
                return label
        return "D5"

    df["classe_distance"] = df["dist_km"].apply(classer)
    return df


# ---------------------------------------------------------------------------
# Enrichissement metadonnees zone exterieure
# ---------------------------------------------------------------------------

def enrichir_zones(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute dep, region, macrozone (M1) et code Insee (COM) pour la zone exterieure.

    Le lookup utilise **COM = code Insee a 5 chiffres** (nom communal absent de ce
    referentiel). Les lignes « 0;0;0;0 » (zones non documentees) sont mises a NaN
    pour l’app (sinon 0, dpt. 00 s’affiche a tort).
    """
    lookup = pd.read_csv(LOOKUP_PATH, sep=None, engine="python")
    lookup = lookup.rename(columns={"ID_ZONAGE": "zone_ext"})
    cols_keep = ["zone_ext"]
    for c in ["DEP", "REG", "M1", "COM"]:
        if c in lookup.columns:
            cols_keep.append(c)

    df = df.merge(
        lookup[cols_keep].drop_duplicates(subset=["zone_ext"]),
        on="zone_ext",
        how="left",
    )
    rename = {"DEP": "dep_ext", "REG": "reg_ext", "M1": "mz_ext", "COM": "com_ext"}
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    for col in ("com_ext", "dep_ext", "reg_ext", "mz_ext"):
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        m = s.notna() & (s == 0)
        df.loc[m, col] = np.nan

    return df


# ---------------------------------------------------------------------------
# Agregation et export
# ---------------------------------------------------------------------------

def agreger_et_exporter(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit en journalier, agrege et exporte (avec colonne `cordon` si présente)."""
    df["nb_pl_jour"] = df["V"] / 365.0

    agg_dict = {
        "nb_pl_jour": ("nb_pl_jour", "sum"),
        "distance_km": ("dist_km", "mean"),
        "dep_ext": ("dep_ext", "first"),
        "reg_ext": ("reg_ext", "first"),
        "mz_ext": ("mz_ext", "first"),
    }
    if "com_ext" in df.columns:
        agg_dict["com_ext"] = ("com_ext", "first")

    gcols = ["zone_ext", "direction", "flux_type", "charge", "classe_distance"]
    if "cordon" in df.columns:
        gcols = ["cordon"] + gcols

    agg = (
        df.groupby(gcols, dropna=False)
        .agg(**agg_dict)
        .reset_index()
    )
    agg["nb_pl_jour"] = agg["nb_pl_jour"].round(2)
    agg["distance_km"] = agg["distance_km"].round(1)

    out = OUTPUT_DIR / CORDON_CSV_NAME
    agg.to_csv(out, index=False)
    print(f"\nExporte: {out} ({len(agg)} lignes)", flush=True)

    total_pl = agg["nb_pl_jour"].sum()
    entrants = agg.loc[agg["direction"] == "entrant", "nb_pl_jour"].sum()
    sortants = agg.loc[agg["direction"] == "sortant", "nb_pl_jour"].sum()
    print(f"  Total PL/j >100km cordon: {total_pl:,.0f}", flush=True)
    print(f"  Entrants: {entrants:,.0f}  Sortants: {sortants:,.0f}", flush=True)
    return agg


# ---------------------------------------------------------------------------
# Ventilation Fos / Sete + bassins (Rhône, extension)
# ---------------------------------------------------------------------------

def _normaliser_nom_feuille(n: str) -> str:
    return n.lower().replace(" ", "_").replace("é", "e").replace("è", "e")


def _lire_premier_id_zone_onglet(
    xlsx: Path, noms_feuille: tuple[str, ...]
) -> int | None:
    """Premier entier > 0 trouvé dans l’onglet (IDs zone OPSAM : typiquement 1re colonne)."""
    if not xlsx.is_file():
        return None
    try:
        xl = pd.ExcelFile(xlsx)
    except Exception as e:
        print(f"  Ouverture Excel impossible {xlsx}: {e}", flush=True)
        return None
    cible = {_normaliser_nom_feuille(n) for n in noms_feuille}
    feuille = None
    for s in xl.sheet_names:
        if _normaliser_nom_feuille(s) in cible:
            feuille = s
            break
    if feuille is None:
        return None
    try:
        df = pd.read_excel(xlsx, sheet_name=feuille, header=None)
    except Exception as e:
        print(f"  Lecture {xlsx.name} / {feuille!r}: {e}", flush=True)
        return None
    if df is None or df.empty:
        return None
    for _i, row in df.iterrows():
        for v in row:
            if v is None or (isinstance(v, float) and np.isnan(v)):
                continue
            if isinstance(v, str) and not str(v).strip():
                continue
            try:
                z = int(float(v))
            except (TypeError, ValueError):
                continue
            if z > 0:
                return z
    return None


def lire_zones_ventilation_opsam_chalon_macon() -> tuple[int, int]:
    """ID agrégat OPSAM pour l’onglet *Bassins* (distinct des listes isochrones 1 h).

    Ordre de remplissage (pour chaque pôle, si encore 0) :

    1. ``data/ventilation_zones_chalon_macon.json`` s’il contient un id non nul.
    2. Fichiers Chalon / Mâcon dans ``PAGNY_REPORT`` (p.ex. ``liste_zones_chalon60min_distincation_paca.xlsx``), onglets
       ``zone_chalon`` / ``zone_macon`` (noms ajustables via ``BASSIN_VENTIL_SHEET_CHALON``,
       ``BASSIN_VENTIL_SHEET_MACON``) — mêmes classeurs que pour le cordon (ex. U:\\\\...\\\\PAGNY_REPORT).
    3. Variables d’environnement ``BASSIN_VENTIL_ZONE_CHALON`` / ``BASSIN_VENTIL_ZONE_MACON``.
    """
    a, b = 0, 0
    p = _DATA_DIR / "ventilation_zones_chalon_macon.json"
    if p.is_file():
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            a = int(obj.get("zone_opsam_chalon", 0) or 0)
            b = int(obj.get("zone_opsam_macon", 0) or 0)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
    n_ch = (
        SHEET_ZONE_AGREGE_CHALON,
        "zone_chalon",
        "Zone_Chalon",
    )
    n_ma = (
        SHEET_ZONE_AGREGE_MACON,
        "zone_macon",
        "zone_mâcon",
        "Zone_Macon",
    )
    ch_xlsx, ma_xlsx = _excel_zones_chalon(), _excel_zones_macon()
    if a == 0:
        z = _lire_premier_id_zone_onglet(ch_xlsx, n_ch)
        if z is not None and z > 0:
            a = z
            print(
                f"  Ventilation: zone agreg. Chalon = {a} (feuille dédiée dans {ch_xlsx})",
                flush=True,
            )
    if b == 0:
        z = _lire_premier_id_zone_onglet(ma_xlsx, n_ma)
        if z is not None and z > 0:
            b = z
            print(
                f"  Ventilation: zone agreg. Macon = {b} (feuille dédiée dans {ma_xlsx})",
                flush=True,
            )
    if a == 0:
        a = int(os.environ.get("BASSIN_VENTIL_ZONE_CHALON", "0") or 0)
    if b == 0:
        b = int(os.environ.get("BASSIN_VENTIL_ZONE_MACON", "0") or 0)
    return a, b


def build_bassins_ventilation(results: dict):
    """Liste unifiee pour l'app (barres, sliders) : Fos, Sete, vallée (Drôme / Isère) + noeuds Saone."""
    fos = results.get("fos", {})
    sete = results.get("sete", {})
    dvm = results.get("drome_valence_montelimar", {})
    isv = results.get("isere_vienne", {})
    chal = results.get("chalon", {})
    macn = results.get("macon", {})
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
        {
            "id": "drome_valence_montelimar",
            "nom": dvm.get(
                "nom",
                "Drome (26) — bassin ports Valence / Montélimar (agregat OPSAM 5135)",
            ),
            "zone_opsam": int(dvm.get("zone_opsam", 5135)),
            "coeff": float(dvm.get("coeff", 0)),
            "source": dvm.get("source", "urssaf"),
            "color": "#00695C",
            "note": dvm.get(
                "note",
                "Part = emploi fret (APE 49/52) des communes portuaires retenues / emploi fret dep. 26. "
                "A ajuster (liste de communes) dans prepare_pagny.py si besoin.",
            ),
        },
        {
            "id": "isere_vienne",
            "nom": isv.get(
                "nom",
                "Isere (38) — bassin port de Vienne (agregat OPSAM 5235)",
            ),
            "zone_opsam": int(isv.get("zone_opsam", 5235)),
            "coeff": float(isv.get("coeff", 0)),
            "source": isv.get("source", "urssaf"),
            "color": "#5E35B1",
            "note": isv.get(
                "note",
                "Part = emploi fret (APE 49/52) des communes du pourtour de Vienne / emploi fret dep. 38. "
                "A ajuster dans prepare_pagny.py si besoin.",
            ),
        },
        {
            "id": "chalon",
            "nom": chal.get(
                "nom",
                "Saone-et-Loire (71) — bassin port de Chalon-sur-Saone",
            ),
            "zone_opsam": int(chal.get("zone_opsam", 0) or 0),
            "coeff": float(chal.get("coeff", 0) or 0),
            "source": chal.get("source", "urssaf"),
            "color": "#2E7D32",
            "note": chal.get(
                "note",
                "Part = emploi fret (49/52) des communes du bassin Chalon / emploi fret dep. 71. "
                "Renseignez `zone_opsam` (agregat OPSAM) via data/ventilation_zones_chalon_macon.json.",
            ),
        },
        {
            "id": "macon",
            "nom": macn.get(
                "nom",
                "Saone-et-Loire (71) — bassin port de Macon",
            ),
            "zone_opsam": int(macn.get("zone_opsam", 0) or 0),
            "coeff": float(macn.get("coeff", 0) or 0),
            "source": macn.get("source", "urssaf"),
            "color": "#6A1B9A",
            "note": macn.get(
                "note",
                "Part = emploi fret (49/52) des communes du bassin Macon / emploi fret dep. 71. "
                "Meme den. que Chalon ; communes a ajuster dans prepare_pagny.py. "
                "`zone_opsam` : data/ventilation_zones_chalon_macon.json.",
            ),
        },
    ]
    out.append(
        {
            "id": "pagny_hinterland",
            "nom": (
                "Influence Aproport / Chalon–Macon–Pagny (optionnel, agreg. OPSAM a renseigner)"
            ),
            "zone_opsam": 0,
            "coeff": 0.0,
            "source": "manuel",
            "color": "#00838F",
            "note": "Definir zone_opsam d’apres lookup_opsam_ite (meme logique que 5124/5125).",
        }
    )
    return out


def calculer_ventilation_fos_sete() -> dict:
    """Coefficients de ventilation (emploi fret URSSAF) : Méditerranée, vallée, Saône (Chalon / Mâcon).

    Fos / Sète : part = emploi fret des communes du zip / emploi fret *région* (départements listés).

    **drome_valence_montelimar** (5135) et **isere_vienne** (5235) : numérateur = communes du
    bassin, dénominateur = emploi fret de tout le **département** 26 ou 38.

    **chalon** et **macon** (dépt. 71) : chacun a sa liste de communes portuaires ; le **dénominateur
    est l’emploi fret de tout le département 71** (même requête URSSAF), comme pour la Drôme / l’Isère.
    Les **IDs d’agrégat OPSAM** affichés dans l’app (barres) se configurent via
    ``ventilation_zones_chalon_macon.json`` (voir ``lire_zones_ventilation_opsam_chalon_macon``) ;
    elles ne sont **pas** les listes d’isochrones 1 h (fichiers Excel de cordon).

    Communes (Insee) : affiner ``BASSIN_DROME_…``, ``BASSIN_ISERE_…``, ``BASSIN_CHALON_71``,
    ``BASSIN_MACON_71`` en tête de cette fonction.
    """

    # --- Mediterranee (inchangé) ---
    ZIP_FOS = {
        "13039": "Fos-sur-Mer",
        "13078": "Port-Saint-Louis-du-Rhone",
        "13077": "Port-de-Bouc",
        "13056": "Martigues",
        "13063": "Miramas",
        "13047": "Istres",
        "13044": "Grans",
        "13097": "Saint-Martin-de-Crau",
    }
    ZIP_SETE = {
        "34301": "Sete",
        "34108": "Frontignan",
        "34023": "Balaruc-les-Bains",
        "34024": "Balaruc-le-Vieux",
        "34150": "Marseillan",
        "34157": "Meze",
        "34003": "Agde",
    }

    # Drôme (26) : bassin de vie Valence côté Drôme (10 comm.) + Montélimar (port)
    BASSIN_DROME_VALENCE_MONTELIMAR = {
        "26038": "Beaumont-Monteux",
        "26042": "Beauvallon",
        "26058": "Bourg-les-Valence",
        "26084": "Chateauneuf-sur-Isere",
        "26113": "Saint-Marcel-les-Valence",
        "26124": "Etoile-sur-Rhone",
        "26170": "Malissard",
        "26198": "Montelimar",
        "26250": "Pont-de-l'Isere",
        "26252": "Portes-les-Valence",
        "26362": "Valence",
    }
    # Isère (38) : pourtour port de Vienne / Rhône
    BASSIN_ISERE_VIENNE = {
        "38087": "Chasse-sur-Rhone",
        "38420": "Saint-Maurice-l'Exil",
        "38487": "Seyssuel",
        "38544": "Vienne",
    }
    # Saône-et-Loire (71) : deux portes (Chalon / Mâcon) — listes a affiner
    BASSIN_CHALON_71 = {
        "71076": "Chalon-sur-Saone",
        "71098": "Crissey",
        "71251": "Saint-Marcel",
        "71077": "Chagny",
        "71099": "Changy",
    }
    BASSIN_MACON_71 = {
        "71270": "Macon",
        "71084": "Charnay-les-Macon",
        "71095": "Creches-sur-Saone",
        "71080": "La-Chapelle-de-Guinchay",
        "71559": "Varennes-les-Macon",
    }
    zc_vent, zm_vent = lire_zones_ventilation_opsam_chalon_macon()

    configs = {
        "fos": {
            "communes": ZIP_FOS,
            "nom": "ZIP Fos-Etang de Berre",
            "deps_emploi": ["04", "05", "06", "13", "83", "84"],
            "region_label": "PACA",
            "zone_opsam": 5125,
        },
        "sete": {
            "communes": ZIP_SETE,
            "nom": "Bassin portuaire Sete-Thau",
            "deps_emploi": [
                "09", "11", "12", "30", "31", "32",
                "34", "46", "48", "65", "66", "81", "82",
            ],
            "region_label": "Occitanie",
            "zone_opsam": 5124,
        },
        "drome_valence_montelimar": {
            "communes": BASSIN_DROME_VALENCE_MONTELIMAR,
            "nom": "Drome (26) — ports Valence / Montélimar",
            "deps_emploi": ["26"],
            "region_label": "Drome",
            "zone_opsam": 5135,
        },
        "isere_vienne": {
            "communes": BASSIN_ISERE_VIENNE,
            "nom": "Isere (38) — port de Vienne (Rhône)",
            "deps_emploi": ["38"],
            "region_label": "Isere",
            "zone_opsam": 5235,
        },
        "chalon": {
            "communes": BASSIN_CHALON_71,
            "nom": "Saone-et-Loire (71) — bassin Chalon-sur-Saone (port)",
            "deps_emploi": ["71"],
            "region_label": "Saone-et-Loire",
            "zone_opsam": int(zc_vent),
        },
        "macon": {
            "communes": BASSIN_MACON_71,
            "nom": "Saone-et-Loire (71) — bassin Macon (port)",
            "deps_emploi": ["71"],
            "region_label": "Saone-et-Loire",
            "zone_opsam": int(zm_vent),
        },
    }

    default_coeff_erreur = {
        "fos": 0.10,
        "sete": 0.05,
        "drome_valence_montelimar": 0.10,
        "isere_vienne": 0.10,
        "chalon": 0.08,
        "macon": 0.08,
    }

    results: dict = {}
    for key, cfg in configs.items():
        dep_filter = " OR ".join(
            f'code_departement="{d}"' for d in cfg["deps_emploi"]
        )
        ape_filter = " OR ".join(
            f'secteur_na88 LIKE \"{s}\"' for s in SECTEURS_FRET
        )
        where = f"({dep_filter}) AND ({ape_filter})"

        params = {
            "where": where,
            "select": "code_commune,effectifs_salaries_2023,effectifs_salaries_2022",
            "delimiter": ";",
            "limit": -1,
        }

        print(f"  URSSAF: emploi fret {cfg['region_label']}...", end=" ", flush=True)
        try:
            r = requests.get(URSSAF_EXPORT_URL, params=params, timeout=120)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text), sep=";")

            year_col = "effectifs_salaries_2023"
            if year_col not in df.columns or df[year_col].isna().all():
                year_col = "effectifs_salaries_2022"

            df[year_col] = pd.to_numeric(df[year_col], errors="coerce").fillna(0)
            df["code_commune"] = df["code_commune"].astype(str).str.zfill(5)

            emploi_region = df[year_col].sum()
            codes_bassin = set(cfg["communes"].keys())
            emploi_bassin = df.loc[
                df["code_commune"].isin(codes_bassin), year_col
            ].sum()

            coeff = emploi_bassin / emploi_region if emploi_region > 0 else 0

            detail_communes = {}
            for code, nom in cfg["communes"].items():
                e = df.loc[df["code_commune"] == code, year_col].sum()
                if e > 0:
                    detail_communes[f"{nom} ({code})"] = int(e)

            print(
                f"bassin={int(emploi_bassin)} ({len(codes_bassin)} communes), "
                f"region={int(emploi_region)}, coeff={coeff:.4f}",
                flush=True,
            )
            for nom_c, e_c in sorted(detail_communes.items(), key=lambda x: -x[1]):
                print(f"    {nom_c}: {e_c}", flush=True)

            results[key] = {
                "nom": cfg["nom"],
                "communes": cfg["communes"],
                "zone_opsam": cfg["zone_opsam"],
                "region_label": cfg["region_label"],
                "coeff": round(coeff, 4),
                "emploi_bassin": int(emploi_bassin),
                "emploi_region": int(emploi_region),
                "detail_communes": detail_communes,
            }
        except Exception as e:
            print(f"ERREUR: {e}", flush=True)
            dcoef = default_coeff_erreur.get(key, 0.05)
            results[key] = {
                "nom": cfg["nom"],
                "communes": cfg["communes"],
                "zone_opsam": cfg["zone_opsam"],
                "region_label": cfg["region_label"],
                "coeff": dcoef,
                "emploi_bassin": 0,
                "emploi_region": 0,
                "detail_communes": {},
                "erreur": str(e),
            }

    # Coefficient fusionne "corridor Saone-Rhone" (Fos + Sete)
    coeff_fos = results["fos"]["coeff"]
    coeff_sete = results["sete"]["coeff"]
    results["corridor"] = {
        "nom": "Corridor Saone-Rhone (ZIP Fos + Sete)",
        "coeff_fos": coeff_fos,
        "coeff_sete": coeff_sete,
        "emploi_bassin_fos": results["fos"]["emploi_bassin"],
        "emploi_bassin_sete": results["sete"]["emploi_bassin"],
    }
    print(
        f"\n  Corridor fusionne: coeff_fos={coeff_fos:.4f}, "
        f"coeff_sete={coeff_sete:.4f}",
        flush=True,
    )
    c_dv = results.get("drome_valence_montelimar", {}).get("coeff", 0)
    c_vi = results.get("isere_vienne", {}).get("coeff", 0)
    c_ch = results.get("chalon", {}).get("coeff", 0)
    c_ma = results.get("macon", {}).get("coeff", 0)
    print(
        f"  Drome 5135 (Valence / Montélimar): coeff={c_dv:.4f} — "
        f"Isère 5235 (Vienne): coeff={c_vi:.4f}",
        flush=True,
    )
    zch = int(results.get("chalon", {}).get("zone_opsam", 0) or 0)
    zma = int(results.get("macon", {}).get("zone_opsam", 0) or 0)
    print(
        f"  Chalon (dep. 71) zone {zch or '(non renseignee)'}: coeff={c_ch:.4f} — "
        f"Mâcon (dep. 71) zone {zma or '(non renseignee)'}: coeff={c_ma:.4f}",
        flush=True,
    )

    results["bassins_ventilation"] = build_bassins_ventilation(results)

    out = OUTPUT_DIR / VENTILATION_JSON_NAME
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nVentilation exportee: {out}", flush=True)
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("PREPARATION ANALYSE CORDON FLUVIAL (PAGNY + CHALON + MACON SI FICHIERS ZONES)")
    print("=" * 60)
    print(f"Dossier PAGNY_REPORT (Excel, GeoJSON) : {PAGNY_REPORT}", flush=True)
    if os.environ.get("PAGNY_REPORT"):
        print("  (surcharge PAGNY_REPORT via variable d'environnement)", flush=True)

    blocs = regler_cordons()
    all_parts = []
    for cordon_id, zones in blocs:
        part = pipeline_un_cordon(zones, cordon_id)
        if not part.empty:
            all_parts.append(part)
    if not all_parts:
        print("Aucun flux apres filtrage — arret.", flush=True)
        return

    print("\n--- Agregation ---", flush=True)
    df = pd.concat(all_parts, ignore_index=True)
    agreger_et_exporter(df)

    print("\n--- Ventilation (Fos / Sète, Drôme, Isère, Chalon / Mâcon 71) ---")
    calculer_ventilation_fos_sete()

    print("\n" + "=" * 60)
    print("TERMINE")
    print("=" * 60)


if __name__ == "__main__":
    main()
