"""
app.py — Dashboard Streamlit d'analyse des macrozones OPSAM.

Pages :
  1. Vue d'ensemble cartographique
  2. Analyse detaillee par macrozone
  3. Comparaison entre macrozones
  4. Analyse par classe de distance
"""

import sys
from pathlib import Path

import gc
import os
import streamlit as st
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from data_loader import (
    CHEMIN_CSV_DEFAUT,
    CHEMIN_SHP_DEFAUT,
    CHEMIN_NB_PL,
    CHEMIN_EMPLOI_FRET,
    CHEMIN_ITE,
    CLASSES_DISTANCE,
    FLUX_LABELS,
    charger_csv_macrozone,
    charger_shapefile_macrozone,
    calculer_metriques,
    preparer_donnees_sankey_macrozone,
    preparer_donnees_distance,
    preparer_donnees_distance_pl,
    calculer_repartition_vl_pl,
    generer_labels_macrozones,
    charger_emploi_detail,
    charger_ite_detail,
    charger_ite_geodata,
    charger_cours_marchandise,
    calculer_score_composite,
    charger_cordon_pagny,
    charger_ventilation_fos_sete,
    charger_isochrone_pagny,
    charger_aires_60min_chalon_macon,
    signature_fichiers_aires_chalon_macon,
    CHEMIN_PAGNY_CORDON,
    CHEMIN_PAGNY_VENT,
    CHEMIN_PAGNY_ISOCHRONE,
    CHEMIN_AIRES_60M_CHALON_MACON,
)
from visualizations import (
    creer_carte_macrozones,
    creer_sankey_macrozone,
    creer_donut_vl_pl,
    creer_donut_pl_detail,
    creer_barres_distance,
    creer_barres_distance_pl,
    creer_heatmap_comparative,
    creer_scatter_transit_pl,
    creer_bar_comparatif,
    creer_barres_toutes_mz_distance,
    creer_profil_distance,
    creer_carte_camemberts_distance,
    creer_carte_score_composite,
    creer_barres_score,
    creer_barres_secteurs,
    creer_radar_macrozone,
    creer_tableau_croise_html,
    creer_carte_pagny_isochrone,
    creer_barres_cordon_distance,
    creer_barres_cordon_origines,
    creer_donut_flux_pagny,
    creer_barres_bassins_ventile,
    creer_contexte_fluvial,
)
from pdf_export import (
    generer_rapport_macrozone,
    generer_rapport_global,
    fusionner_pdfs_bytes,
)

# =========================================================================
# Configuration page
# =========================================================================

st.set_page_config(
    page_title="Macrozones OPSAM — BFC",
    page_icon=":material/map:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Charger le CSS custom
css_path = Path(__file__).parent / "style.css"
if css_path.exists():
    css_content = css_path.read_text(encoding="utf-8")
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

# =========================================================================
# Fonctions utilitaires
# =========================================================================

def fmt_milliers(val):
    """Formate un nombre avec espace comme separateur de milliers."""
    if pd.isna(val):
        return ""
    return f"{int(val):,}".replace(",", " ")


def _mtime_fichier(path: str | Path) -> float:
    """Dernière modification (timestamp) — pour invalider le cache Streamlit si le fichier est remplacé."""
    p = Path(path)
    try:
        return p.stat().st_mtime if p.is_file() else 0.0
    except OSError:
        return 0.0


def _get_mapbox_token() -> str:
    """Token public Mapbox (optionnel) : env ou st.secrets ; sinon carte OSM sans token."""
    t = (os.environ.get("MAPBOX_TOKEN") or "").strip()
    if t:
        return t
    try:
        return (st.secrets.get("MAPBOX_TOKEN") or "").strip()
    except Exception:
        return ""


@st.cache_data(show_spinner="Chargement du CSV...")
def _charger_csv(chemin: str) -> pd.DataFrame:
    return charger_csv_macrozone(chemin)


@st.cache_data(show_spinner="Chargement du shapefile...")
def _charger_shp(chemin: str):
    return charger_shapefile_macrozone(chemin)


@st.cache_data(show_spinner="Calcul des metriques...")
def _calculer_metriques(chemin_csv: str, _nb_pl_mtime: float = 0) -> pd.DataFrame:
    df = _charger_csv(chemin_csv)
    return calculer_metriques(df)


@st.cache_data(show_spinner="Generation des labels...")
def _generer_labels(chemin_shp: str) -> dict:
    return generer_labels_macrozones(chemin_shp)


# =========================================================================
# Sidebar
# =========================================================================

PAGES = {
    "Vue d'ensemble": "bar_chart",
    "Analyse par macrozone": "search",
    "Comparaison": "compare_arrows",
    "Analyse par distance": "route",
    "Contexte & Enrichissement": "hub",
    "Report fluvial": "directions_boat",
}

with st.sidebar:
    st.markdown(
        "<h1 style='color:#E0E0E0; font-size:1.4rem; margin-bottom:0;'>"
        "Macrozones OPSAM</h1>"
        "<p style='color:#90A4AE; font-size:0.85rem; margin-top:4px;'>"
        "Analyse du trafic PL — Bourgogne-Franche-Comte</p>",
        unsafe_allow_html=True,
    )

    st.divider()

    with st.expander("Sources de donnees", expanded=False):
        chemin_csv = st.text_input(
            "Fichier CSV macrozone",
            value=str(CHEMIN_CSV_DEFAUT),
            help="CSV de synthese produit par OPSAM",
        )
        chemin_shp = st.text_input(
            "Zonage macro (SHP, GeoJSON, GPKG ou URL https)",
            value=str(CHEMIN_SHP_DEFAUT),
            help="Fichier lu par GeoPandas : .shp, .geojson, .gpkg ou variable MACROZONE_SHP. "
            "Défaut : data/opsam_zonage_metazone_ite_serm.* (priorité .geojson si présent).",
        )

    try:
        df_raw = _charger_csv(chemin_csv)
        enrich_mtime = sum(
            os.path.getmtime(str(p)) if p.exists() else 0
            for p in [CHEMIN_NB_PL, CHEMIN_EMPLOI_FRET, CHEMIN_ITE]
        )
        metriques = _calculer_metriques(chemin_csv, enrich_mtime)
        labels_mz = _generer_labels(chemin_shp)
    except Exception as e:
        st.error(f"Erreur de chargement : {e}")
        st.stop()

    macrozones_dispo = sorted(metriques["M1"].unique())

    st.divider()

    page = st.radio(
        "Navigation",
        list(PAGES.keys()),
        index=0,
        captions=[
            "Carte et tableau global",
            "Zoom sur une macrozone",
            "Classement et correlations",
            "Profils D1-D5",
            "ITE, emploi fret, scores",
            "Potentiel report modal fluvial",
        ],
    )

# Chargement des points ITE et cours de marchandise pour les cartes
@st.cache_data(show_spinner=False)
def _charger_ite_geo():
    return charger_ite_geodata()

@st.cache_data(show_spinner=False)
def _charger_cours_geo():
    return charger_cours_marchandise()

_ite_geo = _charger_ite_geo()
_cours_geo = _charger_cours_geo()

# =========================================================================
# Page 1 : Vue d'ensemble
# =========================================================================

if page == "Vue d'ensemble":
    st.header("Vue d'ensemble des macrozones")

    # KPI en ligne horizontale
    k1, k2, k3, k4, k5 = st.columns(5)
    nb_mz = len(macrozones_dispo)
    vkm_total = metriques["VKM_milliers"].sum()
    pct_transit_pl_moy = metriques["pct_transit_pl"].mean()
    pct_pl_moy = metriques["pct_pl"].mean()
    pl_total = metriques["nb_pl_total"].sum() if "nb_pl_total" in metriques.columns else 0

    k1.metric("Macrozones", f"{nb_mz}")
    k2.metric("VKM total (TV)", f"{vkm_total:,.0f} k km/j", help="Tous vehicules (VL + PL)")
    k3.metric("% Transit PL moy.", f"{pct_transit_pl_moy:.1f} %", help="Part transit dans les VKM PL")
    k4.metric("% PL moyen (VKM)", f"{pct_pl_moy:.1f} %", help="Part PL dans les VKM")
    k5.metric("PL/jour total BFC", fmt_milliers(pl_total), help="Poids lourds uniquement")

    st.markdown("")

    # Carte pleine largeur avec selecteur en ligne
    col_sel_carte, _ = st.columns([1, 3])
    with col_sel_carte:
        metrique_carte = st.selectbox(
            "Colorer la carte par",
            [
                "pct_transit", "pct_echange", "pct_interne", "pct_pl",
                "VKM_milliers", "vkm_par_km", "pct_longue_distance",
                "nb_pl_echange", "nb_pl_interne", "nb_pl_total",
            ],
            format_func=lambda x: {
                "pct_transit": "% Transit (tous vehicules)",
                "pct_echange": "% Echange (tous vehicules)",
                "pct_interne": "% Interne (tous vehicules)",
                "pct_pl": "% Poids lourds (VKM)",
                "VKM_milliers": "VKM total TV (k km/j)",
                "vkm_par_km": "VKM TV / km infra",
                "pct_longue_distance": "% Longue distance (TV)",
                "nb_pl_echange": "PL/j en echange",
                "nb_pl_interne": "PL/j internes",
                "nb_pl_total": "PL/j total",
            }.get(x, x),
        )

    try:
        gdf = _charger_shp(chemin_shp)
        fig_carte = creer_carte_macrozones(
            gdf, metriques, metrique_carte, labels_mz,
            ite_points=_ite_geo, cours_points=_cours_geo,
        )
        st.plotly_chart(fig_carte, use_container_width=True)
    except Exception as e:
        st.warning(f"Carte indisponible : {e}")

    # Tableau de synthese
    st.subheader("Tableau de synthese")

    cols_display = [
        "M1", "VKM_milliers", "pct_pl", "pct_transit",
        "pct_echange", "pct_interne", "pct_longue_distance",
        "DISTANCE",
    ]
    if "nb_pl_echange" in metriques.columns:
        cols_display += ["nb_pl_echange", "nb_pl_interne", "nb_pl_total"]
    if "emploi_fret" in metriques.columns:
        cols_display += ["emploi_fret", "nb_ite"]

    df_display = metriques[cols_display].copy()
    df_display["DISTANCE"] = df_display["DISTANCE"].round(0)
    df_display.insert(0, "Macrozone", df_display["M1"].map(labels_mz))

    rename_map = {
        "VKM_milliers": "VKM TV (k km/j)",
        "pct_pl": "% PL (VKM)",
        "pct_transit": "% Transit (TV)",
        "pct_echange": "% Echange (TV)",
        "pct_interne": "% Interne (TV)",
        "pct_longue_distance": "% Long. dist. (TV)",
        "DISTANCE": "Reseau (km)",
        "nb_pl_echange": "PL/j echange",
        "nb_pl_interne": "PL/j interne",
        "nb_pl_total": "PL/j total",
        "emploi_fret": "Emplois fret",
        "nb_ite": "Nb ITE",
    }
    df_display = df_display.rename(columns=rename_map)
    df_display = df_display.sort_values("VKM TV (k km/j)", ascending=False)

    # Formatage des colonnes numeriques
    col_config = {
        "VKM TV (k km/j)": st.column_config.NumberColumn(format="%d"),
        "% PL (VKM)": st.column_config.NumberColumn(format="%.1f %%"),
        "% Transit (TV)": st.column_config.NumberColumn(format="%.1f %%"),
        "% Echange (TV)": st.column_config.NumberColumn(format="%.1f %%"),
        "% Interne (TV)": st.column_config.NumberColumn(format="%.1f %%"),
        "% Long. dist. (TV)": st.column_config.NumberColumn(format="%.1f %%"),
        "Reseau (km)": st.column_config.NumberColumn(format="%d"),
    }
    if "PL/j echange" in df_display.columns:
        col_config["PL/j echange"] = st.column_config.NumberColumn(format="%d")
        col_config["PL/j interne"] = st.column_config.NumberColumn(format="%d")
        col_config["PL/j total"] = st.column_config.NumberColumn(format="%d")
    if "Emplois fret" in df_display.columns:
        col_config["Emplois fret"] = st.column_config.NumberColumn(format="%d")
        col_config["Nb ITE"] = st.column_config.NumberColumn(format="%d")

    st.dataframe(
        df_display.drop(columns=["M1"]),
        use_container_width=True,
        height=min(800, len(df_display) * 35 + 60),
        hide_index=True,
        column_config=col_config,
    )

    col_exp1, col_exp2, _ = st.columns([1, 1, 3])
    with col_exp1:
        csv_bytes = df_display.drop(columns=["M1"]).to_csv(
            index=False, sep=";"
        ).encode("utf-8-sig")
        st.download_button(
            ":material/download: Exporter CSV",
            csv_bytes,
            "synthese_macrozones.csv",
            "text/csv",
        )
    with col_exp2:
        if st.button(":material/picture_as_pdf: Rapport PDF global", key="btn_pdf_global"):
            with st.spinner("Generation du PDF..."):
                try:
                    gdf = _charger_shp(chemin_shp)
                    fig_c = creer_carte_macrozones(
                        gdf, metriques, "pct_transit", labels_mz,
                        ite_points=_ite_geo, cours_points=_cours_geo,
                    )
                    fig_h = creer_heatmap_comparative(metriques, labels_mz)
                    fig_s = creer_scatter_transit_pl(metriques, labels_mz)
                    pdf_b = generer_rapport_global(
                        fig_c, fig_h, fig_s, "", chemin_sortie=None
                    )
                    if pdf_b:
                        st.session_state["dl_pdf_global_bytes"] = pdf_b
                except Exception as e:
                    st.error(f"Erreur : {e}")
        if st.session_state.get("dl_pdf_global_bytes"):
            st.download_button(
                "Télécharger le PDF (synthèse globale)",
                st.session_state["dl_pdf_global_bytes"],
                file_name="rapport_global_macrozones.pdf",
                mime="application/pdf",
                key="dl_rapport_global",
            )


# =========================================================================
# Page 2 : Analyse par macrozone
# =========================================================================

elif page == "Analyse par macrozone":
    st.header("Analyse detaillee par macrozone")

    mz_selectionnee = st.selectbox(
        "Choisir une macrozone",
        macrozones_dispo,
        format_func=lambda m: labels_mz.get(m, f"MZ {m}"),
    )

    nom_mz = labels_mz.get(mz_selectionnee, f"Macrozone {mz_selectionnee}")
    row = metriques[metriques["M1"] == mz_selectionnee].iloc[0]

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("VKM total (TV)", f"{row['VKM_milliers']:.0f} k km/j", help="Tous vehicules")
    c2.metric("% Transit PL", f"{row['pct_transit_pl']:.1f} %", help="Part transit dans VKM PL")
    c3.metric("% Echange PL", f"{row['pct_echange_pl']:.1f} %", help="Part echange dans VKM PL")
    c4.metric("% PL (VKM)", f"{row['pct_pl']:.1f} %", help="Part PL dans les VKM")
    if "nb_pl_total" in metriques.columns:
        c5.metric("PL/jour total", fmt_milliers(row.get("nb_pl_total", 0)), help="Poids lourds")
    else:
        c5.metric("% Long. dist. (TV)", f"{row['pct_longue_distance']:.1f} %")
    c6.metric("Emplois fret", fmt_milliers(row.get("emploi_fret", 0)))
    c7.metric("ITE", f"{int(row.get('nb_ite', 0))}")

    st.divider()

    col_sankey, col_donuts = st.columns([3, 1])

    with col_sankey:
        aggr_df = preparer_donnees_sankey_macrozone(df_raw, mz_selectionnee)
        fig_sankey = creer_sankey_macrozone(aggr_df, nom_mz)
        st.plotly_chart(fig_sankey, use_container_width=True)

    with col_donuts:
        repartition = calculer_repartition_vl_pl(df_raw, mz_selectionnee)
        fig_donut = creer_donut_vl_pl(repartition, nom_mz)
        st.plotly_chart(fig_donut, use_container_width=True)

        fig_pl_det = creer_donut_pl_detail(repartition)
        st.plotly_chart(fig_pl_det, use_container_width=True)

    st.divider()

    col_dist, col_pl = st.columns(2)

    with col_dist:
        df_dist = preparer_donnees_distance(df_raw, mz_selectionnee)
        fig_dist = creer_barres_distance(df_dist)
        st.plotly_chart(fig_dist, use_container_width=True)

    with col_pl:
        flux_sel = st.selectbox(
            "Flux PL",
            list(FLUX_LABELS.values()),
            index=0,
            key="flux_pl_detail",
        )
        df_dist_pl = preparer_donnees_distance_pl(df_raw, mz_selectionnee)
        fig_pl_dist = creer_barres_distance_pl(df_dist_pl, flux_sel)
        st.plotly_chart(fig_pl_dist, use_container_width=True)

    # --- Section ITE & Emploi fret de la macrozone ---
    st.divider()
    st.subheader(":material/train: ITE & contexte fret")

    col_ite, col_emploi = st.columns(2)

    with col_ite:
        ite_detail = charger_ite_detail()
        ite_mz = ite_detail[ite_detail["M1"] == mz_selectionnee] if not ite_detail.empty else ite_detail
        nb_ite_mz = len(ite_mz)

        if nb_ite_mz > 0:
            st.markdown(f"**{nb_ite_mz} ITE** dans cette macrozone :")
            cols_show = ["id", "departement", "commune", "raison_sociale"]
            cols_show = [c for c in cols_show if c in ite_mz.columns]
            st.dataframe(
                ite_mz[cols_show].rename(columns={
                    "id": "ID",
                    "departement": "Departement",
                    "commune": "Commune",
                    "raison_sociale": "Raison sociale",
                }),
                use_container_width=True,
                hide_index=True,
                height=min(300, nb_ite_mz * 35 + 60),
            )
        else:
            st.info("Aucune ITE dans cette macrozone.")

    with col_emploi:
        emploi_det = charger_emploi_detail()
        emp_mz = emploi_det[emploi_det["M1"] == mz_selectionnee] if not emploi_det.empty else emploi_det

        if not emp_mz.empty:
            total_emp = emp_mz["emploi"].sum()
            st.markdown(f"**{total_emp:,} emplois fret** (secteurs generateurs)".replace(",", " "))
            top5 = emp_mz.nlargest(5, "emploi")[["label_secteur", "emploi", "nb_etab"]].rename(columns={
                "label_secteur": "Secteur",
                "emploi": "Emplois",
                "nb_etab": "Etablissements",
            })
            st.dataframe(top5, use_container_width=True, hide_index=True)
        else:
            st.info("Pas de donnees emploi fret pour cette macrozone.")

    st.divider()
    if st.button(":material/picture_as_pdf: Rapport PDF macrozone", key="btn_pdf_mz"):
        with st.spinner("Generation du PDF..."):
            try:
                pdf_b = generer_rapport_macrozone(
                    mz_selectionnee,
                    nom_mz,
                    fig_sankey,
                    fig_donut,
                    fig_dist,
                    fig_pl_dist,
                    repartition,
                    chemin_sortie=None,
                )
                if pdf_b:
                    st.session_state["dl_pdf_mz_bytes"] = pdf_b
                    st.session_state["dl_pdf_mz_name"] = (
                        f"macrozone_{mz_selectionnee}.pdf"
                    )
            except Exception as e:
                st.error(f"Erreur : {e}")
    if st.session_state.get("dl_pdf_mz_bytes") is not None:
        st.download_button(
            "Télécharger le PDF (macrozone courante)",
            st.session_state["dl_pdf_mz_bytes"],
            file_name=st.session_state.get("dl_pdf_mz_name", "macrozone.pdf"),
            mime="application/pdf",
            key="dl_rapport_mz",
        )


# =========================================================================
# Page 3 : Comparaison
# =========================================================================

elif page == "Comparaison":
    st.header("Comparaison entre macrozones")

    tab_bar, tab_heat, tab_scatter = st.tabs([
        ":material/leaderboard: Classement",
        ":material/grid_on: Heatmap",
        ":material/scatter_plot: Transit vs PL",
    ])

    with tab_bar:
        col_sel, col_n = st.columns([2, 1])
        with col_sel:
            options_comp = [
                "VKM_milliers", "pct_transit", "pct_pl",
                "pct_longue_distance", "vkm_par_km",
            ]
            if "nb_pl_echange" in metriques.columns:
                options_comp += ["nb_pl_echange", "nb_pl_interne", "nb_pl_total"]

            metrique_comp = st.selectbox(
                "Metrique a comparer",
                options_comp,
                format_func=lambda x: {
                    "VKM_milliers": "VKM total TV (k km/j)",
                    "pct_transit": "% Transit (tous vehicules)",
                    "pct_pl": "% Poids lourds (VKM)",
                    "pct_longue_distance": "% Longue distance (TV)",
                    "vkm_par_km": "VKM TV / km infra",
                    "nb_pl_echange": "PL/j en echange",
                    "nb_pl_interne": "PL/j internes",
                    "nb_pl_total": "PL/j total",
                }.get(x, x),
                key="metrique_bar",
            )
        with col_n:
            top_n = st.slider("Nombre de macrozones", 5, 36, 20)

        fig_bar = creer_bar_comparatif(metriques, metrique_comp, top_n, labels_mz)
        st.plotly_chart(fig_bar, use_container_width=True)

    with tab_heat:
        fig_heat = creer_heatmap_comparative(metriques, labels_mz)
        st.plotly_chart(fig_heat, use_container_width=True)

    with tab_scatter:
        fig_scatter = creer_scatter_transit_pl(metriques, labels_mz)
        st.plotly_chart(fig_scatter, use_container_width=True)


# =========================================================================
# Page 4 : Analyse par distance
# =========================================================================

elif page == "Analyse par distance":
    st.header("Analyse par classe de distance")

    tab_carte, tab_global, tab_profil = st.tabs([
        ":material/map: Carte par macrozone",
        ":material/stacked_bar_chart: Vue globale",
        ":material/show_chart: Profil comparatif",
    ])

    with tab_carte:
        try:
            gdf_dist = _charger_shp(chemin_shp)
            fig_cam = creer_carte_camemberts_distance(
                gdf_dist, df_raw, labels_mz,
                ite_points=_ite_geo, cours_points=_cours_geo,
            )
            st.plotly_chart(fig_cam, use_container_width=True)
            st.caption(
                "Chaque camembert represente une macrozone. "
                "La **taille** est proportionnelle au VKM PL total, "
                "les **parts** montrent la repartition par classe de distance."
            )
        except Exception as e:
            st.warning(f"Carte camemberts indisponible : {e}")

    with tab_global:
        flux_code_sel = st.selectbox(
            "Type de flux",
            list(FLUX_LABELS.keys()),
            format_func=lambda k: FLUX_LABELS[k],
            key="flux_global",
        )
        fig_all = creer_barres_toutes_mz_distance(df_raw, flux_code_sel, labels_mz)
        st.plotly_chart(fig_all, use_container_width=True)

    with tab_profil:
        col_flux, col_mz = st.columns([1, 2])
        with col_flux:
            flux_profil = st.selectbox(
                "Flux",
                list(FLUX_LABELS.keys()),
                format_func=lambda k: FLUX_LABELS[k],
                key="flux_profil",
            )
        with col_mz:
            mz_profil = st.multiselect(
                "Macrozones a comparer",
                macrozones_dispo,
                default=macrozones_dispo[:5],
                format_func=lambda m: labels_mz.get(m, f"MZ {m}"),
            )

        if mz_profil:
            fig_profil = creer_profil_distance(df_raw, mz_profil, flux_profil, labels_mz)
            st.plotly_chart(fig_profil, use_container_width=True)
        else:
            st.info("Selectionnez au moins une macrozone.")

    st.divider()
    if st.button(
        ":material/picture_as_pdf: Generer tous les PDF (telechargement unique)",
        key="btn_all_pdf_mz",
    ):
        progress = st.progress(0, text="Generation en cours...")
        pdfs: list[bytes] = []
        for i, mz in enumerate(macrozones_dispo):
            progress.progress((i + 1) / len(macrozones_dispo), text=f"Macrozone {mz}...")
            try:
                nom = labels_mz.get(mz, f"Macrozone {mz}")
                aggr = preparer_donnees_sankey_macrozone(df_raw, mz)
                f_san = creer_sankey_macrozone(aggr, nom)
                rep = calculer_repartition_vl_pl(df_raw, mz)
                f_don = creer_donut_vl_pl(rep, nom)
                f_dist = creer_barres_distance(preparer_donnees_distance(df_raw, mz))
                f_pl = creer_barres_distance_pl(
                    preparer_donnees_distance_pl(df_raw, mz), "Echange"
                )
                b = generer_rapport_macrozone(
                    mz, nom, f_san, f_don, f_dist, f_pl, rep, chemin_sortie=None
                )
                if b:
                    pdfs.append(b)
            except Exception as e:
                st.warning(f"MZ {mz} : {e}")
            gc.collect()

        progress.empty()
        if pdfs:
            try:
                st.session_state["dl_pdf_complet"] = fusionner_pdfs_bytes(pdfs)
                st.success(
                    f"PDF complet : {len(pdfs)} macrozone(s) — telechargement ci-dessous."
                )
            except Exception as e:
                st.error(f"Fusion PDF : {e}")
    if st.session_state.get("dl_pdf_complet") is not None:
        st.download_button(
            "Telecharger le rapport complet (toutes les macrozones)",
            st.session_state["dl_pdf_complet"],
            file_name="rapport_complet_macrozones.pdf",
            mime="application/pdf",
            key="dl_pdf_toutes_mz",
        )


# =========================================================================
# Page 5 : Contexte & Enrichissement
# =========================================================================

elif page == "Contexte & Enrichissement":
    st.header("Contexte & Enrichissement")
    st.caption(
        "Croisement des donnees trafic OPSAM avec les donnees emploi "
        "(URSSAF/open data) et les Infrastructures Terminales Embranchees (ITE)."
    )

    tab_score, tab_secteurs, tab_ite, tab_croise = st.tabs([
        ":material/priority_high: Score de priorite",
        ":material/factory: Secteurs fret",
        ":material/train: ITE",
        ":material/grid_view: Tableau croise",
    ])

    # --- Onglet 1 : Score de priorite ---
    with tab_score:
        col_seuil, col_w1, col_w2, col_w3 = st.columns([1, 1, 1, 1])
        with col_seuil:
            seuil_dist = st.selectbox(
                "Distance minimale PL reportable",
                list(CLASSES_DISTANCE.keys()),
                index=2,
                format_func=lambda k: CLASSES_DISTANCE[k],
                key="seuil_score",
            )
        with col_w1:
            w_trafic = st.slider("Poids trafic PL", 0, 100, 50, 5, key="w_trafic", help="Ponderation du trafic PL reportable")
        with col_w2:
            w_emploi = st.slider("Poids emploi fret", 0, 100, 30, 5, key="w_emploi", help="Ponderation de l'emploi fret")
        with col_w3:
            w_ite = st.slider("Poids ITE", 0, 100, 20, 5, key="w_ite", help="Ponderation des ITE")

        total_w = w_trafic + w_emploi + w_ite
        if total_w > 0:
            st.caption(
                f"Ponderation : trafic PL **{w_trafic/total_w*100:.0f}%** · "
                f"emploi fret **{w_emploi/total_w*100:.0f}%** · "
                f"ITE **{w_ite/total_w*100:.0f}%**"
            )
        else:
            st.warning("Au moins un poids doit etre superieur a 0.")
            w_trafic, w_emploi, w_ite = 50, 30, 20

        metriques_score = calculer_score_composite(
            metriques, seuil_dist,
            poids_trafic=w_trafic, poids_emploi=w_emploi, poids_ite=w_ite,
        )

        fig_bar_score = creer_barres_score(
            metriques_score, labels_mz, top_n=25,
            poids_trafic=w_trafic, poids_emploi=w_emploi, poids_ite=w_ite,
        )
        st.plotly_chart(fig_bar_score, use_container_width=True)

        try:
            gdf = _charger_shp(chemin_shp)
            fig_carte_score = creer_carte_score_composite(
                gdf, metriques_score, labels_mz, "score_composite",
                ite_points=_ite_geo, cours_points=_cours_geo,
            )
            st.plotly_chart(fig_carte_score, use_container_width=True)
        except Exception as e:
            st.warning(f"Carte indisponible : {e}")

        st.subheader("Detail des scores")
        df_scores = metriques_score[
            ["M1", "score_composite", "score_trafic", "score_emploi", "score_ite",
             "vkm_pl_reportable", "emploi_fret", "nb_ite"]
        ].copy()
        df_scores.insert(0, "Macrozone", df_scores["M1"].map(labels_mz))
        df_scores = df_scores.sort_values("score_composite", ascending=False)

        st.dataframe(
            df_scores.drop(columns=["M1"]),
            use_container_width=True,
            height=min(700, len(df_scores) * 35 + 60),
            hide_index=True,
            column_config={
                "score_composite": st.column_config.NumberColumn("Score", format="%.1f"),
                "score_trafic": st.column_config.NumberColumn("Score trafic", format="%.1f"),
                "score_emploi": st.column_config.NumberColumn("Score emploi", format="%.1f"),
                "score_ite": st.column_config.NumberColumn("Score ITE", format="%.1f"),
                "vkm_pl_reportable": st.column_config.NumberColumn("VKM PL report.", format="%d"),
                "emploi_fret": st.column_config.NumberColumn("Emplois fret", format="%d"),
                "nb_ite": st.column_config.NumberColumn("Nb ITE", format="%d"),
            },
        )

    # --- Onglet 2 : Secteurs fret ---
    with tab_secteurs:
        emploi_detail = charger_emploi_detail()

        if emploi_detail.empty:
            st.warning(
                "Donnees emploi non disponibles. "
                "Lancez `python fetch_flores.py` pour les telecharger."
            )
        else:
            st.subheader("Analyse par secteur generateur de fret")
            col_mz_sec, _ = st.columns([1, 2])
            with col_mz_sec:
                mz_secteur = st.selectbox(
                    "Macrozone",
                    macrozones_dispo,
                    format_func=lambda m: labels_mz.get(m, f"MZ {m}"),
                    key="mz_secteur",
                )

            col_bar, col_radar = st.columns([3, 2])

            with col_bar:
                fig_sec = creer_barres_secteurs(emploi_detail, mz_secteur, labels_mz)
                st.plotly_chart(fig_sec, use_container_width=True)

            with col_radar:
                metriques_score_radar = calculer_score_composite(
                    metriques, seuil_dist,
                    poids_trafic=w_trafic, poids_emploi=w_emploi, poids_ite=w_ite,
                )
                fig_radar = creer_radar_macrozone(metriques_score_radar, mz_secteur, labels_mz)
                st.plotly_chart(fig_radar, use_container_width=True)

            st.divider()
            st.subheader("Repartition emploi fret - toutes macrozones")

            top_secteurs = (
                emploi_detail.groupby("label_secteur")["emploi"]
                .sum()
                .nlargest(10)
                .index.tolist()
            )
            pivot = emploi_detail[emploi_detail["label_secteur"].isin(top_secteurs)].pivot_table(
                index="M1", columns="label_secteur", values="emploi", aggfunc="sum", fill_value=0
            )
            pivot.insert(0, "Macrozone", pivot.index.map(labels_mz))
            pivot = pivot.sort_index()

            st.dataframe(
                pivot,
                use_container_width=True,
                height=min(600, len(pivot) * 35 + 60),
                hide_index=True,
            )

    # --- Onglet 3 : ITE ---
    with tab_ite:
        ite_detail = charger_ite_detail()

        if ite_detail.empty:
            st.warning(
                "Donnees ITE non disponibles. "
                "Lancez `python prepare_ite.py` pour les preparer."
            )
        else:
            st.subheader("Infrastructures Terminales Embranchees (ITE)")

            k1, k2, k3 = st.columns(3)
            k1.metric("ITE totales BFC", f"{len(ite_detail)}")
            nb_mz_avec_ite = ite_detail[ite_detail["M1"] > 0]["M1"].nunique()
            k2.metric("Macrozones avec ITE", f"{nb_mz_avec_ite}")
            ite_moy = len(ite_detail) / nb_mz_avec_ite if nb_mz_avec_ite > 0 else 0
            k3.metric("ITE / macrozone (moy.)", f"{ite_moy:.1f}")

            try:
                gdf = _charger_shp(chemin_shp)
                fig_carte_ite = creer_carte_macrozones(
                    gdf, metriques, "nb_ite", labels_mz,
                    ite_points=_ite_geo, cours_points=_cours_geo,
                )
                st.plotly_chart(fig_carte_ite, use_container_width=True)
            except Exception as e:
                st.warning(f"Carte ITE indisponible : {e}")

            st.subheader("Liste des ITE par macrozone")
            col_filt, _ = st.columns([1, 3])
            with col_filt:
                filtre_mz = st.selectbox(
                    "Filtrer par macrozone",
                    [0] + macrozones_dispo,
                    format_func=lambda m: "Toutes" if m == 0 else labels_mz.get(m, f"MZ {m}"),
                    key="filtre_ite",
                )

            df_ite_show = ite_detail.copy()
            if filtre_mz > 0:
                df_ite_show = df_ite_show[df_ite_show["M1"] == filtre_mz]

            df_ite_show["Macrozone"] = df_ite_show["M1"].map(labels_mz).fillna("Hors MZ")
            cols_show = ["id", "departement", "commune", "raison_sociale", "Macrozone"]
            cols_show = [c for c in cols_show if c in df_ite_show.columns]

            st.dataframe(
                df_ite_show[cols_show],
                use_container_width=True,
                height=min(600, len(df_ite_show) * 35 + 60),
                hide_index=True,
            )

    # --- Onglet 4 : Tableau croise ---
    with tab_croise:
        metriques_score_croise = calculer_score_composite(
            metriques, seuil_dist,
            poids_trafic=w_trafic, poids_emploi=w_emploi, poids_ite=w_ite,
        )

        total_w_c = w_trafic + w_emploi + w_ite
        st.subheader("Tableau croise - Indicateurs visuels")
        st.caption(
            "Vert = faible enjeu, Orange = enjeu modere, Rouge = enjeu fort. "
            f"Score composite = {w_trafic/total_w_c*100:.0f}% trafic PL + "
            f"{w_emploi/total_w_c*100:.0f}% emploi fret + "
            f"{w_ite/total_w_c*100:.0f}% ITE."
        )

        html_table = creer_tableau_croise_html(metriques_score_croise, labels_mz)
        st.markdown(html_table, unsafe_allow_html=True)

# =========================================================================
# Page 6 : Report fluvial (Saône / couloir — Pagny, Chalon, Mâcon, …)
# =========================================================================

elif page == "Report fluvial":
    st.header("Report fluvial — vallée de la Saône et couloir fluvial")

    @st.cache_data(show_spinner=False)
    def _charger_pagny_data(chemin: str, _mtime: float = 0.0):
        """_mtime sert de clé de cache : un nouveau `git pull` (fichier remplacé) met à jour les chiffres sans « Clear cache »."""
        return charger_cordon_pagny(chemin or None)

    @st.cache_data(show_spinner=False)
    def _charger_pagny_vent(_mtime: float = 0.0):
        return charger_ventilation_fos_sete()

    @st.cache_data(show_spinner=False)
    def _charger_pagny_iso(_mtime: float = 0.0):
        return charger_isochrone_pagny()

    @st.cache_data(show_spinner=False)
    def _charger_aires_60m_cm(fichiers_sig: str):
        return charger_aires_60min_chalon_macon()

    st.caption(
        "Fichier agrege (CSV) : par defaut `pagny_cordon_flows.csv` a cote de l'app "
        "(colonnes `cordon` = pagny, chalon, macon apres `prepare_pagny`). "
        "Surcharge possible pour comparaison (ex. `output_dev`)."
    )
    chemin_cordon_ui = st.text_input(
        "Fichier flux cordon (CSV agrege)",
        value=str(CHEMIN_PAGNY_CORDON),
        key="pagny_csv_path",
        help="Laissez le chemin par defaut en usage courant. Utilisez une copie pour A/B test.",
    )
    _path_cordon = chemin_cordon_ui.strip() or str(CHEMIN_PAGNY_CORDON)
    df_pagny = _charger_pagny_data(_path_cordon, _mtime_fichier(_path_cordon))
    vent_data = _charger_pagny_vent(_mtime_fichier(CHEMIN_PAGNY_VENT))
    gdf_iso = _charger_pagny_iso(_mtime_fichier(CHEMIN_PAGNY_ISOCHRONE))
    aires_60m_cm = _charger_aires_60m_cm(signature_fichiers_aires_chalon_macon())
    _mb_tok = _get_mapbox_token() or None

    st.caption(
        f"**Aires 60 min (carte, Chalon / Mâcon) :** dossier lu = `{CHEMIN_AIRES_60M_CHALON_MACON}`. "
        f"Fichiers détectés : {len(aires_60m_cm)}. Surcharge : `MACROZONE_AIRES_60M_CHALON_MACON_DIR`."
    )
    if not aires_60m_cm:
        st.info(
            f"Aucun `.geojson` (ou `.json` GeoJSON) chargé. Vérifiez que le dossier existe, "
            f"puis *Clear cache* (menu) et rechargez. Dossier attendu : `{CHEMIN_AIRES_60M_CHALON_MACON}`."
        )

    cordon_opts = ["Tous"]
    if not df_pagny.empty and "cordon" in df_pagny.columns:
        cordon_opts += sorted({str(c) for c in df_pagny["cordon"].dropna().unique()})
    sel_cordon = st.selectbox(
        "Cordon d'analyse (port / isochrone 1 h) — s'applique aux onglets ci-dessous",
        options=cordon_opts,
        key="cordon_fluvial_sel",
        help="Filtre les lignes du CSV selon la colonne `cordon` (pagny, chalon, macon). Régénérez le CSV avec `prepare_pagny` apres ajout des listes de zones.",
    )
    df_fl = (
        df_pagny
        if sel_cordon == "Tous" or df_pagny.empty
        else df_pagny[df_pagny["cordon"].astype(str) == str(sel_cordon)]
    )

    tab_ctx, tab_cordon, tab_detail, tab_fos = st.tabs([
        ":material/water: Contexte fluvial",
        ":material/my_location: Vue cordon",
        ":material/analytics: Analyse detaillee",
        ":material/anchor: Bassins / corridor Rhone",
    ])

    # --- Onglet 1 : Contexte fluvial ---
    with tab_ctx:
        st.subheader("Le corridor Saone-Rhone")
        col_txt, col_chart = st.columns([3, 2])
        with col_txt:
            st.markdown("""
**Pagny-le-Chateau** est une plateforme portuaire trimodale (route, rail, voie d'eau)
geree par **Aproport** sur la Saone. Elle s'inscrit dans le corridor fluvial
**Saone-Rhone** reliant la Bourgogne-Franche-Comte aux ports mediterraneens
de **Marseille-Fos** et **Sete**.

**Chiffres cles (2024) :**
- **Aproport** (Chalon + Macon + Pagny) : **2,7 Mt** — route 58%, rail 24%, voie d'eau 18%
- **Marseille-Fos** : **70,5 Mt** (1er port francais), conteneurs +9%
- **Sete** : **~5,8 Mt** (+3,6%), 10e annee de croissance

**Equivalences environnementales :**
- 1 barge fluviale (2 500 t) = **200 camions**
- Transport fluvial = **-50% CO2** vs route
- Aproport : > 100 000 t CO2 evitees / an

*Sources : Aproport, SDES, Mer et Marine*
            """)
        with col_chart:
            fig_ctx = creer_contexte_fluvial()
            st.plotly_chart(fig_ctx, use_container_width=True)

        st.info(
            "Les indicateurs d’onglet utilisent le **filtre « Cordon »** (Pagny, Chalon, Mâcon — "
            "selon le CSV) et des flux PL **> 100 km** à travers l’isochrone 1 h. "
            "Générez le CSV multi-cordons dans `prepare_pagny` avec les listes de zones OPSAM."
        )

    # --- Onglet 2 : Vue cordon ---
    with tab_cordon:
        if df_pagny.empty:
            st.warning(
                "Donnees cordon non disponibles. "
                "Executez `prepare_pagny.py` pour generer le CSV (Pagny, puis Chalon/Mâcon si listes de zones fournies)."
            )
        elif df_fl.empty:
            st.warning(
                "Aucun flux pour ce cordon. Verifiez le filtre ci-dessus ou regenerez le CSV apres `prepare_pagny`."
            )
        else:
            total_pl = df_fl["nb_pl_jour"].sum()
            pl_in = df_fl.loc[df_fl["direction"] == "entrant", "nb_pl_jour"].sum()
            pl_out = df_fl.loc[df_fl["direction"] == "sortant", "nb_pl_jour"].sum()
            dist_moy = (
                (df_fl["distance_km"] * df_fl["nb_pl_jour"]).sum()
                / total_pl if total_pl > 0 else 0
            )
            pl_400 = df_fl.loc[
                df_fl["classe_distance"].isin(["D4", "D5"]), "nb_pl_jour"
            ].sum()
            pct_400 = pl_400 / total_pl * 100 if total_pl > 0 else 0

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("PL/j total (>100km)", f"{total_pl:,.0f}")
            k2.metric("PL entrants", f"{pl_in:,.0f}")
            k3.metric("PL sortants", f"{pl_out:,.0f}")
            k4.metric("Distance moy.", f"{dist_moy:,.0f} km")
            k5.metric("% > 400 km", f"{pct_400:.1f}%")

            try:
                gdf_bg = _charger_shp(chemin_shp)
            except Exception:
                gdf_bg = None

            fig_map = creer_carte_pagny_isochrone(
                gdf_iso, df_fl, gdf_dissolved=gdf_bg,
                ite_points=_ite_geo, cours_points=_cours_geo,
                mapbox_token=_mb_tok,
                aires_chalandise_60m=aires_60m_cm,
            )
            st.plotly_chart(fig_map, use_container_width=True)

            _cm_names = ", ".join(
                (x.get("path").name if x.get("path") is not None else "")
                for x in aires_60m_cm
            ) if aires_60m_cm else ""
            st.caption(
                f"Cordon actuel : **{sel_cordon}** — isochrones 1 h (trace) + flux PL > 100 km du CSV (cordon / matrice). "
                f"Matrice : OPSAM Ref2024 (annuel / 365). "
                + (f" GeoJSON Chalon/Mâcon : {_cm_names}." if _cm_names else "")
            )

    # --- Onglet 3 : Analyse detaillee ---
    with tab_detail:
        if df_pagny.empty:
            st.warning("Donnees cordon non disponibles.")
        elif df_fl.empty:
            st.warning("Aucun flux pour le cordon selectionne.")
        else:
            col_dir, col_flux = st.columns(2)
            with col_dir:
                sel_dir = st.selectbox(
                    "Direction", ["Tous", "Entrant", "Sortant"],
                    key="pagny_dir",
                )
            with col_flux:
                sel_flux = st.selectbox(
                    "Type de flux",
                    ["Tous", "Echange", "Transit", "Interne"],
                    key="pagny_flux",
                )

            df_f = df_fl.copy()
            if sel_dir != "Tous":
                df_f = df_f[df_f["direction"] == sel_dir.lower()]
            flux_map = {"Echange": "E", "Transit": "T", "Interne": "I"}
            if sel_flux != "Tous":
                df_f = df_f[df_f["flux_type"] == flux_map[sel_flux]]

            col_bar, col_donut = st.columns([3, 1])
            with col_bar:
                fig_dist = creer_barres_cordon_distance(df_f)
                st.plotly_chart(fig_dist, use_container_width=True)
            with col_donut:
                fig_donut = creer_donut_flux_pagny(df_f)
                st.plotly_chart(fig_donut, use_container_width=True)

            fig_orig = creer_barres_cordon_origines(df_f)
            st.plotly_chart(fig_orig, use_container_width=True)

            st.subheader("Tableau detaille")
            df_show = df_f.copy()
            rename_cols = {
                "cordon": "Cordon",
                "zone_ext": "Zone ext.",
                "direction": "Direction",
                "flux_type": "Flux",
                "charge": "Charge",
                "classe_distance": "Classe dist.",
                "nb_pl_jour": "PL/jour",
                "distance_km": "Distance (km)",
                "dep_ext": "Dep.",
                "mz_ext": "Macrozone",
            }
            cols_disp = [c for c in rename_cols if c in df_show.columns]
            df_show = df_show[cols_disp].rename(columns=rename_cols)
            df_show = df_show.sort_values("PL/jour", ascending=False)

            st.dataframe(
                df_show,
                use_container_width=True,
                height=min(500, len(df_show) * 35 + 60),
                hide_index=True,
            )

            csv_data = df_show.to_csv(index=False).encode("utf-8")
            st.download_button(
                ":material/download: Telecharger CSV",
                csv_data,
                "pagny_cordon_detail.csv",
                "text/csv",
            )

    # --- Onglet 4 : Bassins / corridor (Fos, Sete, extension vallée du Rhône) ---
    with tab_fos:
        if df_pagny.empty:
            st.warning("Donnees cordon non disponibles.")
        elif df_fl.empty:
            st.warning("Aucun flux pour le cordon selectionne — Fos/Sete necessitent un volume cote filtre actuel.")
        else:
            fos_info = vent_data.get("fos", {})
            sete_info = vent_data.get("sete", {})
            corridor_info = vent_data.get("corridor", {})
            bassins = vent_data.get("bassins_ventilation", [])

            st.subheader("Corridor Saone-Rhone — bassins (mediterranee + extension)")
            st.caption(
                "Agrégats Méditerranée (**5125**, **5124**), vallée (**5135**, **5235**), et **Chalon / Mâcon** "
                "(dép. 71) : *coeff.* = part emploi fret (URSSAF) des communes du bassin / département, comme "
                "pour Fos. Renseignez `zone_opsam` pour Chalon / Mâcon (fichier `data/ventilation_zones_chalon_macon.json` "
                "ou variables d’environnement, puis regénérez `prepare_pagny`). Filtre **Cordon = Tous** si "
                "les `zone_ext` concernés sont sur plusieurs exports."
            )

            def _def_coeff(b) -> float:
                bid = b.get("id", "")
                c = float(b.get("coeff", 0) or 0)
                if bid == "fos":
                    return float(corridor_info.get("coeff_fos", fos_info.get("coeff", c)))
                if bid == "sete":
                    return float(corridor_info.get("coeff_sete", sete_info.get("coeff", c)))
                return c

            coeffs: dict[str, float] = {}
            nb_b = len(bassins)
            if nb_b == 0:
                st.info("Aucun `bassins_ventilation` — regenerez le JSON (prepare_pagny).")
            else:
                use_cols = nb_b <= 4
                col_list = st.columns(min(2, nb_b)) if use_cols else None
                for idx, b in enumerate(bassins):
                    bid = b.get("id", f"b{idx}")
                    dft = _def_coeff(b)
                    zid = int(b.get("zone_opsam") or 0)
                    help_txt = b.get("note") or ""
                    if zid <= 0:
                        help_txt = (
                            (help_txt + " ") if help_txt else ""
                        ) + "Renseignez `zone_opsam` pour activer barres et metriques."
                    label = f"Coeff. {b.get('nom', bid)}"[:80]
                    if col_list is not None:
                        c = col_list[idx % len(col_list)]
                        with c:
                            coeffs[bid] = st.slider(
                                label, 0.0, 1.0, dft, 0.01,
                                key=f"w_bassin_{bid}_fluvial", help=help_txt[:500],
                            )
                    else:
                        coeffs[bid] = st.slider(
                            label, 0.0, 1.0, dft, 0.01,
                            key=f"w_bassin_{bid}_fluvial", help=help_txt[:500],
                        )

            dvm_info = vent_data.get("drome_valence_montelimar", {})
            isv_info = vent_data.get("isere_vienne", {})
            chal_info = vent_data.get("chalon", {})
            macon_info = vent_data.get("macon", {})
            with st.expander("Communes / détail (bassins URSSAF — Méditerranée + vallée)"):
                col_cf, col_cs = st.columns(2)
                with col_cf:
                    st.markdown(f"**{fos_info.get('nom', 'ZIP Fos')}**")
                    det_fos = fos_info.get("detail_communes", {})
                    if det_fos:
                        for nom_c, emp in sorted(det_fos.items(), key=lambda x: -x[1]):
                            st.markdown(f"- {nom_c} : **{emp:,}** emplois fret")
                    else:
                        for code, nom in fos_info.get("communes", {}).items():
                            st.markdown(f"- {nom} ({code})")
                with col_cs:
                    st.markdown(f"**{sete_info.get('nom', 'Bassin Sete')}**")
                    det_sete = sete_info.get("detail_communes", {})
                    if det_sete:
                        for nom_c, emp in sorted(det_sete.items(), key=lambda x: -x[1]):
                            st.markdown(f"- {nom_c} : **{emp:,}** emplois fret")
                    else:
                        for code, nom in sete_info.get("communes", {}).items():
                            st.markdown(f"- {nom} ({code})")
                c26, c38 = st.columns(2)
                with c26:
                    st.markdown(
                        f"**{dvm_info.get('nom', 'Drome 5135 — Valence / Montélimar')}** "
                        f"— *coeff.* {dvm_info.get('coeff', 0)} "
                        f"(bassin {dvm_info.get('emploi_bassin', 0):,} / dep. 26 {dvm_info.get('emploi_region', 0):,})"
                    )
                    det_d = dvm_info.get("detail_communes", {})
                    if det_d:
                        for nom_c, emp in sorted(det_d.items(), key=lambda x: -x[1]):
                            st.caption(f"- {nom_c} : **{emp:,}** empl. fret")
                    else:
                        for code, nom in dvm_info.get("communes", {}).items():
                            st.caption(f"- {nom} ({code})")
                with c38:
                    st.markdown(
                        f"**{isv_info.get('nom', 'Isère 5235 — Vienne')}** "
                        f"— *coeff.* {isv_info.get('coeff', 0)} "
                        f"(bassin {isv_info.get('emploi_bassin', 0):,} / dep. 38 {isv_info.get('emploi_region', 0):,})"
                    )
                    det_i = isv_info.get("detail_communes", {})
                    if det_i:
                        for nom_c, emp in sorted(det_i.items(), key=lambda x: -x[1]):
                            st.caption(f"- {nom_c} : **{emp:,}** empl. fret")
                    else:
                        for code, nom in isv_info.get("communes", {}).items():
                            st.caption(f"- {nom} ({code})")
                c71a, c71b = st.columns(2)
                with c71a:
                    st.markdown(
                        f"**{chal_info.get('nom', 'Chalon — 71')}** (zone {chal_info.get('zone_opsam', 0) or '—'})"
                        f" — *coeff.* {chal_info.get('coeff', 0)} ; "
                        f"empl. bassin {int(chal_info.get('emploi_bassin') or 0):,} / dep. 71 "
                        f"{int(chal_info.get('emploi_region') or 0):,}"
                    )
                    det_ch = chal_info.get("detail_communes", {})
                    if det_ch:
                        for nom_c, emp in sorted(det_ch.items(), key=lambda x: -x[1]):
                            st.caption(f"- {nom_c} : **{emp:,}** empl. fret")
                    else:
                        for code, nom in chal_info.get("communes", {}).items():
                            st.caption(f"- {nom} ({code})")
                with c71b:
                    st.markdown(
                        f"**{macon_info.get('nom', 'Mâcon — 71')}** (zone {macon_info.get('zone_opsam', 0) or '—'})"
                        f" — *coeff.* {macon_info.get('coeff', 0)} ; "
                        f"empl. bassin {int(macon_info.get('emploi_bassin') or 0):,} / dep. 71 "
                        f"{int(macon_info.get('emploi_region') or 0):,}"
                    )
                    det_ma = macon_info.get("detail_communes", {})
                    if det_ma:
                        for nom_c, emp in sorted(det_ma.items(), key=lambda x: -x[1]):
                            st.caption(f"- {nom_c} : **{emp:,}** empl. fret")
                    else:
                        for code, nom in macon_info.get("communes", {}).items():
                            st.caption(f"- {nom} ({code})")

            pl_corridor = 0.0
            for b in bassins:
                zid = int(b.get("zone_opsam") or 0)
                if zid <= 0:
                    continue
                bid = b.get("id", "")
                c = float(coeffs.get(bid, 0.0))
                pl_br = float(df_fl.loc[df_fl["zone_ext"] == zid, "nb_pl_jour"].sum())
                pl_corridor += pl_br * c

            m0, m1, _ = st.columns(3)
            m0.metric(
                "PL/j somme (bassins a zone > 0, ventiles)",
                f"{pl_corridor:,.0f}",
                help="Somme (PL/j brut x coeff.) pour chaque bassin avec zone_opsam connu",
            )
            m1.caption(
                "Déposez `ventilation_zones_chalon_macon.json` (IDs agrégat OPSAM) + regen. "
                "`prepare_pagny` — ou ajustez les `zone_opsam` dans le JSON de ventilation."
            )

            fig_fs = creer_barres_bassins_ventile(df_fl, bassins, coeffs)
            st.plotly_chart(fig_fs, use_container_width=True)

            zone_ids = [int(b.get("zone_opsam") or 0) for b in bassins if int(b.get("zone_opsam") or 0) > 0]
            df_sud = df_fl[df_fl["zone_ext"].isin(zone_ids)] if zone_ids else df_fl.iloc[0:0]
            if not df_sud.empty and zone_ids:
                label_z = {int(b.get("zone_opsam")): b.get("nom", str(b.get("id"))) for b in bassins if int(b.get("zone_opsam") or 0) > 0}
                coeff_z = {int(b.get("zone_opsam")): float(coeffs.get(b.get("id"), 0) or 0) for b in bassins if int(b.get("zone_opsam") or 0) > 0}
                st.subheader("Détail des flux par bassin (zones agrégées OPSAM)")
                df_sud_show = df_sud.copy()
                df_sud_show["bassin"] = df_sud_show["zone_ext"].map(
                    lambda z: label_z.get(int(z), f"Zone {z}")
                )
                df_sud_show["coeff"] = df_sud_show["zone_ext"].map(
                    lambda z: coeff_z.get(int(z), 0.0)
                )
                df_sud_show["pl_estime"] = (
                    df_sud_show["nb_pl_jour"] * df_sud_show["coeff"]
                ).round(1)
                cols_fs = [
                    "bassin", "direction", "flux_type", "classe_distance",
                    "nb_pl_jour", "coeff", "pl_estime", "distance_km",
                ]
                cols_fs = [c for c in cols_fs if c in df_sud_show.columns]
                rename_fs = {
                    "bassin": "Bassin / zone",
                    "direction": "Direction",
                    "flux_type": "Flux",
                    "classe_distance": "Classe dist.",
                    "nb_pl_jour": "PL/j brut region",
                    "coeff": "Coeff. ventilation",
                    "pl_estime": "PL/j estime bassin",
                    "distance_km": "Distance (km)",
                }
                df_sud_show = df_sud_show[cols_fs].rename(columns=rename_fs)
                df_sud_show = df_sud_show.sort_values("PL/j brut region", ascending=False)
                st.dataframe(
                    df_sud_show, use_container_width=True,
                    height=min(400, max(20, len(df_sud_show)) * 35 + 60),
                    hide_index=True,
                )

            st.caption(
                "Méthodologie : emploi fret (APE 49+52) du pôle retenu / emploi fret du périmètre (région ou dep.), "
                "puis **PL estimé = PL brut (zone_ext)** × **coeff** (Fos, Sète, 5135, 5235, Chalon, Mâcon). "
                "Dép. **71** : Chalon / Mâcon = deux numérateurs, **même** dénominateur (tout le 71). "
                "Modif. communes dans `prepare_pagny.py` ; *coeff* ici (curseurs) = surcharge. "
            )
