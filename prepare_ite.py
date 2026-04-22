"""Prepare les donnees ITE et cours de marchandise par macrozone.

- ITE : jointure spatiale shapefile ITE ↔ macrozones
- Cours de marchandise : geocodage via API Geo + rattachement macrozone
"""

import geopandas as gpd
import pandas as pd
import requests
import time
from pathlib import Path
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SHP_ITE = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\ITE et plateformes\shp\ITE_BFC.shp"
)
SHP_MZ = Path(
    r"U:\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\metazones_ITE_SERM\opsam_zonage_metazone_ite_serm.shp"
)
CSV_COURS = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\ITE et plateformes\cours_marchandises__BFC_pour_localisation.csv"
)
LOOKUP_PATH = Path(
    r"\\nas-bfc\COMMUN\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL"
    r"\2_INPUT\DATA\ITE et plateformes\lookup_ opsam_ite\lookup_dep_com_epci_macrozone.csv"
)
OUTPUT_DIR = Path(__file__).parent

API_GEO_URL = "https://geo.api.gouv.fr/communes"


def _geocoder_communes(codes_insee: list[str]) -> dict[str, tuple[float, float]]:
    """Geocode une liste de codes INSEE via l'API Geo (centroide commune)."""
    coords = {}
    for code in codes_insee:
        try:
            r = requests.get(
                f"{API_GEO_URL}/{code}",
                params={"fields": "centre", "format": "json"},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                centre = data.get("centre", {}).get("coordinates")
                if centre:
                    coords[code] = (centre[1], centre[0])  # lat, lon
            time.sleep(0.1)
        except Exception:
            pass
    return coords


def _preparer_ite(gdf_mz_dissolved):
    """Jointure spatiale ITE ↔ macrozones, export CSV + coordonnees."""
    gdf_ite = gpd.read_file(str(SHP_ITE))
    print(f"ITE chargees: {len(gdf_ite)}", flush=True)

    gdf_ite_proj = gdf_ite.to_crs(gdf_mz_dissolved.crs)
    joined = gpd.sjoin(gdf_ite_proj, gdf_mz_dissolved, how="left", predicate="within")

    # Detail par ITE
    ite_detail = joined[["id", "departemen", "commune", "derniere r", "statut", "MA_ITE"]].copy()
    ite_detail = ite_detail.rename(columns={
        "departemen": "departement",
        "derniere r": "raison_sociale",
        "MA_ITE": "M1",
    })

    # Extraire lat/lon depuis la geometrie WGS84
    joined_wgs = joined.to_crs(epsg=4326)
    ite_detail["lat"] = joined_wgs.geometry.y
    ite_detail["lon"] = joined_wgs.geometry.x

    ite_detail.to_csv(OUTPUT_DIR / "ite_detail_par_macrozone.csv", index=False)

    # Agregat par macrozone
    ite_par_mz = (
        joined.groupby("MA_ITE")
        .agg(
            nb_ite=("id", "count"),
            noms_ite=("derniere r", lambda x: " | ".join(sorted(x.astype(str).unique()))),
        )
        .reset_index()
        .rename(columns={"MA_ITE": "M1"})
    )
    ite_par_mz.to_csv(OUTPUT_DIR / "ite_par_macrozone.csv", index=False)
    print(f"ITE par macrozone:", flush=True)
    print(ite_par_mz[["M1", "nb_ite"]].sort_values("nb_ite", ascending=False).to_string(index=False), flush=True)

    return ite_detail


def _preparer_cours(gdf_mz_dissolved):
    """Geocode et rattache les cours de marchandise aux macrozones."""
    try:
        df_cours = pd.read_csv(CSV_COURS, encoding="latin-1", sep=None, engine="python")
    except Exception as e:
        print(f"Cours marchandises non charge: {e}", flush=True)
        return

    code_col = [c for c in df_cours.columns if "insee" in c.lower() or "code" in c.lower()]
    if not code_col:
        print("Pas de colonne code INSEE dans le CSV cours.", flush=True)
        return

    col_insee = code_col[0]
    df_cours[col_insee] = df_cours[col_insee].astype(str).str.zfill(5)
    print(f"\nCours marchandises: {len(df_cours)} sites", flush=True)

    # Rattachement macrozone via lookup
    if LOOKUP_PATH.exists():
        lookup = pd.read_csv(LOOKUP_PATH, sep=None, engine="python")
        lookup["COM"] = lookup["COM"].astype(str).str.zfill(5)
        df_cours = df_cours.merge(
            lookup[["COM", "M1"]].drop_duplicates("COM"),
            left_on=col_insee, right_on="COM", how="left",
        )
        df_cours["M1"] = df_cours["M1"].fillna(0).astype(int)
    else:
        print(f"Lookup introuvable ({LOOKUP_PATH}), macrozone non rattachee.", flush=True)
        df_cours["M1"] = 0

    # Geocodage via API Geo
    codes_uniques = df_cours[col_insee].unique().tolist()
    print(f"Geocodage de {len(codes_uniques)} communes...", flush=True)
    coords = _geocoder_communes(codes_uniques)
    print(f"  {len(coords)} communes geocodees.", flush=True)

    df_cours["lat"] = df_cours[col_insee].map(lambda c: coords.get(c, (None, None))[0])
    df_cours["lon"] = df_cours[col_insee].map(lambda c: coords.get(c, (None, None))[1])

    # Renommer les colonnes pour uniformiser
    rename_map = {}
    for c in df_cours.columns:
        cl = c.lower()
        if "commune" in cl or "comuine" in cl:
            rename_map[c] = "commune"
        elif "site" in cl:
            rename_map[c] = "site"
        elif "etat" in cl:
            rename_map[c] = "etat"
    df_cours = df_cours.rename(columns=rename_map)

    cols_out = [col_insee, "commune", "site", "etat", "M1", "lat", "lon"]
    cols_out = [c for c in cols_out if c in df_cours.columns]
    df_out = df_cours[cols_out].copy()
    df_out = df_out.rename(columns={col_insee: "code_insee"})
    df_out.to_csv(OUTPUT_DIR / "cours_marchandise_detail.csv", index=False, encoding="utf-8")
    print(f"Cours marchandise exporte: {len(df_out)} sites", flush=True)
    print(df_out[["code_insee", "site", "M1", "lat", "lon"]].head(10).to_string(index=False), flush=True)


def main():
    # Charger macrozones (reference commune)
    gdf_mz = gpd.read_file(str(SHP_MZ))
    gdf_mz = gdf_mz.dropna(subset=["MA_ITE"])
    gdf_mz["MA_ITE"] = gdf_mz["MA_ITE"].astype(int)
    gdf_mz_dissolved = gdf_mz.dissolve(by="MA_ITE", as_index=False)[["MA_ITE", "geometry"]]

    _preparer_ite(gdf_mz_dissolved)
    _preparer_cours(gdf_mz_dissolved)

    print("\nTermine.", flush=True)


if __name__ == "__main__":
    main()
