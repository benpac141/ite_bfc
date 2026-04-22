"""
visualizations.py — Graphiques Plotly pour le dashboard macrozone OPSAM.

Carte choropleth, Sankey, donuts, barres, heatmap, scatter, profils distance.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import json

from data_loader import (
    CLASSES_DISTANCE,
    FLUX_LABELS,
    CHEMIN_SHP_DEFAUT,
    generer_labels_macrozones,
)

# ---------------------------------------------------------------------------
# Design system
# ---------------------------------------------------------------------------

FONT_FAMILY = "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
FONT_FAMILY_BOLD = "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"

# Palette principale
BLEU_PRIMAIRE = "#1565C0"
BLEU_SECONDAIRE = "#42A5F5"
GRIS_FONCE = "#1A1A2E"
GRIS_MOYEN = "#5A6A7A"
GRIS_CLAIR = "#F5F6F8"

COULEUR_VL = "#4CAF50"
COULEUR_PL = "#E53935"
COULEUR_PL_CHARGE = "#C62828"
COULEUR_PL_VIDE = "#EF9A9A"

COULEURS_FLUX = {
    "Echange": "#1976D2",
    "Transit": "#F57C00",
    "Interne": "#388E3C",
}

COULEURS_VOIE = {
    "Autoroutes": "#C62828",
    "Voies nationales": "#F57C00",
    "Voies departementales": "#388E3C",
    "Voies locales": "#546E7A",
}

COULEURS_DISTANCE = {
    "D1": "#1B5E20",
    "D2": "#66BB6A",
    "D3": "#FFD54F",
    "D4": "#FF7043",
    "D5": "#C62828",
}

LABELS_DISTANCE_COURTS = {
    "D1": "< 100 km",
    "D2": "100-200 km",
    "D3": "200-400 km",
    "D4": "400-1000 km",
    "D5": "> 1000 km",
}

LAYOUT_COMMUN = dict(
    font=dict(family=FONT_FAMILY, size=13, color=GRIS_FONCE),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
)


def _appliquer_axes(fig, xgrid=True, ygrid=True):
    """Applique un style d'axe coherent."""
    commun = dict(
        showline=False,
        zeroline=False,
        gridcolor="#E8ECF0",
        gridwidth=1,
        tickfont=dict(size=11, color=GRIS_MOYEN),
        title_font=dict(size=12, color=GRIS_MOYEN),
    )
    fig.update_xaxes(showgrid=xgrid, **commun)
    fig.update_yaxes(showgrid=ygrid, **commun)
    return fig


# ---------------------------------------------------------------------------
# Carte choropleth
# ---------------------------------------------------------------------------

def _ajouter_marqueurs_ite_cours(fig, ite_points=None, cours_points=None):
    """Ajoute les marqueurs ITE et cours de marchandise sur une carte Mapbox.

    Utilise des cercles (seul symbole supporte par carto-positron)
    avec des couleurs et contours distincts.
    """
    if ite_points is not None and not ite_points.empty:
        fig.add_trace(go.Scattermapbox(
            lat=ite_points["lat"],
            lon=ite_points["lon"],
            mode="markers",
            marker=dict(
                size=10,
                color="#C62828",
                opacity=0.85,
            ),
            name="ITE (embranchement)",
            text=ite_points.get("raison_sociale", ite_points.get("commune", "")),
            hovertemplate=(
                "<b>ITE</b> — %{text}<br>"
                "Commune : %{customdata[0]}<br>"
                "<extra></extra>"
            ),
            customdata=ite_points[["commune"]].values if "commune" in ite_points.columns else None,
            showlegend=True,
        ))

    if cours_points is not None and not cours_points.empty:
        fig.add_trace(go.Scattermapbox(
            lat=cours_points["lat"],
            lon=cours_points["lon"],
            mode="markers",
            marker=dict(
                size=12,
                color="#FF8F00",
                opacity=0.9,
            ),
            name="Cours marchandise",
            text=cours_points.get("site", cours_points.get("commune", "")),
            hovertemplate=(
                "<b>Cours marchandise</b> — %{text}<br>"
                "Commune : %{customdata[0]}<br>"
                "Etat : %{customdata[1]}<br>"
                "<extra></extra>"
            ),
            customdata=(
                cours_points[["commune", "etat"]].values
                if {"commune", "etat"}.issubset(cours_points.columns)
                else None
            ),
            showlegend=True,
        ))

    return fig


def creer_carte_macrozones(
    gdf_dissolved,
    metriques: pd.DataFrame,
    colonne: str = "pct_transit",
    labels_mz: dict | None = None,
    ite_points=None,
    cours_points=None,
) -> go.Figure:
    """Carte choropleth Plotly des macrozones colorees par la metrique choisie."""
    merged = gdf_dissolved.merge(metriques, left_on="MA_ITE", right_on="M1", how="left")

    if labels_mz:
        merged["label"] = merged["MA_ITE"].map(labels_mz)
    else:
        merged["label"] = "MZ " + merged["MA_ITE"].astype(str)

    titres_metriques = {
        "pct_transit": "% Transit (tous vehicules)",
        "pct_echange": "% Echange (tous vehicules)",
        "pct_interne": "% Interne (tous vehicules)",
        "pct_pl": "% Poids lourds (VKM)",
        "VKM_milliers": "VKM tous vehicules (k km/j)",
        "vkm_par_km": "VKM tous vehicules / km infra",
        "pct_longue_distance": "% Longue distance TV (>400 km)",
        "nb_pl_echange": "Nb PL/j en echange",
        "nb_pl_interne": "Nb PL/j internes",
        "nb_pl_total": "Nb PL/j total",
        "nb_ite": "Nb ITE",
    }
    titre_colorbar = titres_metriques.get(colonne, colonne)

    is_pct = colonne.startswith("pct_")
    if is_pct:
        color_scale = [
            [0, "#E3F2FD"], [0.25, "#90CAF9"], [0.5, "#42A5F5"],
            [0.75, "#1565C0"], [1, "#0D47A1"],
        ]
    elif "nb_pl" in colonne:
        color_scale = [
            [0, "#FFF3E0"], [0.25, "#FFB74D"], [0.5, "#F57C00"],
            [0.75, "#E65100"], [1, "#BF360C"],
        ]
    else:
        color_scale = [
            [0, "#E8F5E9"], [0.25, "#81C784"], [0.5, "#43A047"],
            [0.75, "#2E7D32"], [1, "#1B5E20"],
        ]

    geojson = json.loads(merged.to_json())

    fig = px.choropleth_mapbox(
        merged,
        geojson=geojson,
        locations=merged.index,
        color=colonne,
        hover_name="label",
        hover_data={
            "VKM_milliers": ":.0f",
            "pct_transit": ":.1f",
            "pct_pl": ":.1f",
            "DISTANCE": ":.0f",
        },
        color_continuous_scale=color_scale,
        mapbox_style="carto-positron",
        zoom=6,
        center={"lat": 47.0, "lon": 5.5},
        opacity=0.75,
        labels={colonne: titre_colorbar},
    )

    _ajouter_marqueurs_ite_cours(fig, ite_points, cours_points)

    fig.update_layout(
        font=dict(family=FONT_FAMILY, size=12, color=GRIS_FONCE),
        margin=dict(l=0, r=0, t=10, b=0),
        height=520,
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_colorbar=dict(
            title=dict(text=titre_colorbar, font=dict(size=11)),
            thickness=14,
            len=0.55,
            bgcolor="rgba(255,255,255,0.85)",
            borderwidth=0,
            tickfont=dict(size=10),
        ),
        legend=dict(
            yanchor="top", y=0.98, xanchor="left", x=0.01,
            bgcolor="rgba(255,255,255,0.85)",
            font=dict(size=11),
        ),
    )

    return fig


# ---------------------------------------------------------------------------
# Diagramme Sankey
# ---------------------------------------------------------------------------

def _wrap_label(label: str, max_chars: int = 20) -> str:
    if not isinstance(label, str) or len(label) <= max_chars:
        return label
    mots = label.split(" ")
    if len(mots) < 2:
        return label
    mid = len(mots) // 2
    return " ".join(mots[:mid]) + "<br>" + " ".join(mots[mid:])


def _hex_to_rgba(hex_color: str, opacity: float = 0.7) -> str:
    r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{opacity})"


def creer_sankey_macrozone(
    aggr_df: pd.DataFrame,
    nom_macrozone: str,
) -> go.Figure:
    """Diagramme Sankey a 2 niveaux."""
    all_labels_raw = list(
        dict.fromkeys(aggr_df["source"].tolist() + aggr_df["target"].tolist())
    )
    label_to_idx = {label: i for i, label in enumerate(all_labels_raw)}
    display_labels = [_wrap_label(lbl) for lbl in all_labels_raw]

    node_colors = []
    for label in all_labels_raw:
        if label == "Trafic total":
            node_colors.append(GRIS_FONCE)
        elif label in COULEURS_VOIE:
            node_colors.append(COULEURS_VOIE[label])
        elif label in COULEURS_FLUX:
            node_colors.append(COULEURS_FLUX[label])
        else:
            node_colors.append("#78909C")

    link_colors = []
    for _, row in aggr_df.iterrows():
        target = row["target"]
        if target in COULEURS_VOIE:
            link_colors.append(_hex_to_rgba(COULEURS_VOIE[target], 0.5))
        elif target in COULEURS_FLUX:
            link_colors.append(_hex_to_rgba(COULEURS_FLUX[target], 0.5))
        else:
            link_colors.append("rgba(100,100,100,0.35)")

    fig = go.Figure(
        go.Sankey(
            arrangement="snap",
            valueformat=",.0f",
            valuesuffix=" k km/j",
            textfont=dict(size=13, color=GRIS_FONCE, family=FONT_FAMILY),
            node=dict(
                pad=22,
                thickness=22,
                line=dict(color="white", width=1.5),
                label=display_labels,
                color=node_colors,
            ),
            link=dict(
                source=[label_to_idx[s] for s in aggr_df["source"]],
                target=[label_to_idx[t] for t in aggr_df["target"]],
                value=aggr_df["values"].tolist(),
                color=link_colors,
            ),
        )
    )

    fig.update_layout(
        title=dict(
            text=f"<b>Trafic routier (tous vehicules)</b> — {nom_macrozone}",
            x=0.5, xanchor="center",
            font=dict(size=16, family=FONT_FAMILY, color=GRIS_FONCE),
        ),
        font=dict(size=13, family=FONT_FAMILY),
        margin=dict(l=30, r=30, t=65, b=20),
        height=460,
        paper_bgcolor="rgba(0,0,0,0)",
    )

    fig.update_traces(
        textfont=dict(size=12, color=GRIS_FONCE, family=FONT_FAMILY),
        selector=dict(type="sankey"),
    )

    return fig


# ---------------------------------------------------------------------------
# Donut VL / PL
# ---------------------------------------------------------------------------

def creer_donut_vl_pl(repartition: dict, nom_macrozone: str) -> go.Figure:
    labels = ["Vehicules legers", "Poids lourds"]
    values = [repartition["pct_vl"], repartition["pct_pl"]]
    colors = [COULEUR_VL, COULEUR_PL]

    vkm_vl_k = repartition["vkm_vl"] / 1000
    vkm_pl_k = repartition["vkm_pl"] / 1000
    total_milliers = repartition["total_vkm"] / 1000

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.45,
            marker=dict(colors=colors, line=dict(color="white", width=3)),
            textinfo="label+percent",
            textfont=dict(size=13, color="white", family=FONT_FAMILY),
            textposition="inside",
            hovertemplate=(
                "<b>%{label}</b><br>"
                "%{percent}<br>"
                "Volume : %{customdata} k km/jour"
                "<extra></extra>"
            ),
            customdata=[f"{vkm_vl_k:,.0f}", f"{vkm_pl_k:,.0f}"],
        )
    )

    fig.update_layout(
        showlegend=False,
        annotations=[dict(
            text=f"<b>{total_milliers:,.0f}</b><br><span style='font-size:10px;color:{GRIS_MOYEN}'>k km/jour</span>",
            x=0.5, y=0.5, font_size=15, font_color=GRIS_FONCE,
            showarrow=False, align="center",
        )],
        margin=dict(t=10, b=10, l=10, r=10),
        height=280,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    return fig


def creer_donut_pl_detail(repartition: dict) -> go.Figure:
    labels = ["PL charges", "PL vides"]
    values = [repartition["vkm_pl_charges"], repartition["vkm_pl_vides"]]
    colors = [COULEUR_PL_CHARGE, COULEUR_PL_VIDE]
    total_pl_k = sum(values) / 1000

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.45,
            marker=dict(colors=colors, line=dict(color="white", width=3)),
            textinfo="label+percent",
            textfont=dict(size=12, color="white", family=FONT_FAMILY),
            textposition="inside",
        )
    )

    fig.update_layout(
        showlegend=False,
        annotations=[dict(
            text=f"<b>{total_pl_k:,.0f}</b><br><span style='font-size:10px;color:{GRIS_MOYEN}'>k km/j PL</span>",
            x=0.5, y=0.5, font_size=13, font_color=GRIS_FONCE,
            showarrow=False, align="center",
        )],
        margin=dict(t=10, b=10, l=10, r=10),
        height=260,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ---------------------------------------------------------------------------
# Barres empilees par classe de distance
# ---------------------------------------------------------------------------

def creer_barres_distance(df_dist: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    for d_code in reversed(list(CLASSES_DISTANCE.keys())):
        subset = df_dist[df_dist["classe"] == d_code]
        fig.add_trace(go.Bar(
            y=subset["flux"],
            x=subset["vkm_milliers"],
            name=LABELS_DISTANCE_COURTS[d_code],
            orientation="h",
            marker_color=COULEURS_DISTANCE[d_code],
            text=subset["vkm_milliers"].apply(lambda v: f"{v:,.0f}" if v >= 1 else ""),
            textposition="inside",
            textfont=dict(size=11, color="white", family=FONT_FAMILY),
        ))

    fig.update_layout(
        **LAYOUT_COMMUN,
        barmode="stack",
        title=dict(
            text="<b>Repartition par distance (PL)</b>",
            font=dict(size=14, family=FONT_FAMILY),
        ),
        xaxis_title="VKM PL (k km/j)",
        yaxis=dict(autorange="reversed"),
        legend=dict(
            orientation="h", y=-0.18, x=0.5, xanchor="center",
            font=dict(size=10), bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=90, r=20, t=50, b=65),
        height=300,
    )
    _appliquer_axes(fig, ygrid=False)

    return fig


def creer_barres_distance_pl(df_dist_pl: pd.DataFrame, flux_selectionne: str = "Echange") -> go.Figure:
    df_f = df_dist_pl[df_dist_pl["flux"] == flux_selectionne]
    if df_f.empty:
        return go.Figure()

    fig = go.Figure()
    for d_code in reversed(list(CLASSES_DISTANCE.keys())):
        subset = df_f[df_f["classe"] == d_code]
        fig.add_trace(go.Bar(
            y=subset["type_pl"],
            x=subset["vkm_milliers"],
            name=LABELS_DISTANCE_COURTS[d_code],
            orientation="h",
            marker_color=COULEURS_DISTANCE[d_code],
            text=subset["vkm_milliers"].apply(lambda v: f"{v:,.0f}" if v >= 0.5 else ""),
            textposition="inside",
            textfont=dict(size=10, color="white", family=FONT_FAMILY),
        ))

    fig.update_layout(
        **LAYOUT_COMMUN,
        barmode="stack",
        title=dict(
            text=f"<b>Poids lourds par distance</b> — {flux_selectionne}",
            font=dict(size=13, family=FONT_FAMILY),
        ),
        xaxis_title="VKM PL (k km/j)",
        legend=dict(
            orientation="h", y=-0.22, x=0.5, xanchor="center",
            font=dict(size=10), bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=100, r=20, t=50, b=65),
        height=260,
    )
    _appliquer_axes(fig, ygrid=False)
    return fig


# ---------------------------------------------------------------------------
# Heatmap comparative
# ---------------------------------------------------------------------------

def creer_heatmap_comparative(
    metriques: pd.DataFrame,
    labels_mz: dict | None = None,
) -> go.Figure:
    cols = ["pct_transit", "pct_echange", "pct_interne", "pct_pl", "pct_longue_distance"]
    col_labels = ["% Transit (TV)", "% Echange (TV)", "% Interne (TV)", "% PL", "% Long. dist. (TV)"]

    df_heat = metriques.set_index("M1")[cols].copy()
    df_heat.columns = col_labels

    if labels_mz:
        df_heat.index = [labels_mz.get(m, f"MZ {m}") for m in df_heat.index]
    else:
        df_heat.index = [f"MZ {m}" for m in df_heat.index]

    df_heat = df_heat.sort_values("% Transit (TV)", ascending=True)

    color_scale = [
        [0, "#E3F2FD"], [0.2, "#90CAF9"], [0.4, "#42A5F5"],
        [0.6, "#1E88E5"], [0.8, "#1565C0"], [1.0, "#0D47A1"],
    ]

    fig = px.imshow(
        df_heat.values,
        x=col_labels,
        y=df_heat.index.tolist(),
        color_continuous_scale=color_scale,
        aspect="auto",
        text_auto=".0f",
    )

    fig.update_traces(
        textfont=dict(size=11, color="white"),
    )

    fig.update_layout(
        **LAYOUT_COMMUN,
        title=dict(
            text="<b>Profil compare des macrozones</b> (TV = tous vehicules)",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        margin=dict(l=220, r=20, t=55, b=40),
        height=max(450, len(df_heat) * 24 + 100),
        coloraxis_colorbar=dict(title="%", thickness=12, len=0.5),
    )
    fig.update_xaxes(side="top", tickfont=dict(size=11, color=GRIS_MOYEN))
    fig.update_yaxes(tickfont=dict(size=10, color=GRIS_FONCE))

    return fig


# ---------------------------------------------------------------------------
# Scatter transit vs PL
# ---------------------------------------------------------------------------

def creer_scatter_transit_pl(
    metriques: pd.DataFrame,
    labels_mz: dict | None = None,
) -> go.Figure:
    df = metriques.copy()
    if labels_mz:
        df["label"] = df["M1"].map(labels_mz)
    else:
        df["label"] = "MZ " + df["M1"].astype(str)

    fig = px.scatter(
        df,
        x="pct_transit",
        y="pct_pl",
        size="VKM_milliers",
        color="pct_longue_distance",
        hover_name="label",
        hover_data={"VKM_milliers": ":.0f", "pct_transit": ":.1f", "pct_pl": ":.1f"},
        color_continuous_scale=[
            [0, "#E3F2FD"], [0.5, "#1E88E5"], [1, "#0D47A1"],
        ],
        size_max=45,
        labels={
            "pct_transit": "% Transit (TV)",
            "pct_pl": "% Poids lourds (VKM)",
            "VKM_milliers": "VKM TV (k km/j)",
            "pct_longue_distance": "% Long. dist. (TV)",
        },
    )

    fig.update_traces(
        marker=dict(line=dict(width=1, color="white")),
    )

    fig.update_layout(
        **LAYOUT_COMMUN,
        title=dict(
            text="<b>% Transit (TV) vs % Poids lourds</b> par macrozone",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        margin=dict(l=60, r=20, t=60, b=50),
        height=460,
        coloraxis_colorbar=dict(thickness=12, len=0.5),
    )
    _appliquer_axes(fig)

    return fig


# ---------------------------------------------------------------------------
# Bar chart comparatif (top N)
# ---------------------------------------------------------------------------

def creer_bar_comparatif(
    metriques: pd.DataFrame,
    colonne: str,
    top_n: int = 15,
    labels_mz: dict | None = None,
) -> go.Figure:
    df = metriques.nlargest(top_n, colonne).sort_values(colonne, ascending=True).copy()

    if labels_mz:
        df["label"] = df["M1"].map(labels_mz)
    else:
        df["label"] = "MZ " + df["M1"].astype(str)

    titres = {
        "VKM_milliers": "VKM tous vehicules (k km/j)",
        "pct_transit": "% Transit (tous vehicules)",
        "pct_pl": "% Poids lourds (VKM)",
        "pct_longue_distance": "% Longue distance (tous vehicules)",
        "vkm_par_km": "VKM tous vehicules / km infra",
        "nb_pl_echange": "Nb PL/j en echange",
        "nb_pl_interne": "Nb PL/j internes",
        "nb_pl_total": "Nb PL/j total",
    }

    max_val = df[colonne].max()
    colors = []
    for v in df[colonne]:
        ratio = v / max_val if max_val > 0 else 0
        if ratio > 0.8:
            colors.append("#0D47A1")
        elif ratio > 0.5:
            colors.append("#1565C0")
        elif ratio > 0.3:
            colors.append("#42A5F5")
        else:
            colors.append("#90CAF9")

    fig = go.Figure(go.Bar(
        y=df["label"],
        x=df[colonne],
        orientation="h",
        marker_color=colors,
        text=df[colonne].apply(lambda v: f"{v:,.0f}" if v >= 1 else f"{v:.1f}"),
        textposition="outside",
        textfont=dict(size=11, family=FONT_FAMILY, color=GRIS_FONCE),
    ))

    fig.update_layout(
        **LAYOUT_COMMUN,
        title=dict(
            text=f"<b>{titres.get(colonne, colonne)}</b>",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        xaxis_title=titres.get(colonne, colonne),
        margin=dict(l=220, r=70, t=55, b=40),
        height=max(350, top_n * 28 + 100),
    )
    _appliquer_axes(fig, ygrid=False)

    return fig


# ---------------------------------------------------------------------------
# Stacked bars toutes macrozones par distance
# ---------------------------------------------------------------------------

def creer_barres_toutes_mz_distance(
    df_raw: pd.DataFrame,
    flux_code: str = "T",
    labels_mz: dict | None = None,
) -> go.Figure:
    df_mz = df_raw[df_raw["M1"] != 0].copy()

    agg_rows = []
    for mz in sorted(df_mz["M1"].unique()):
        sub = df_mz[df_mz["M1"] == mz]
        row = {"M1": mz}
        for d in CLASSES_DISTANCE:
            val = 0
            for charge in ["C", "V"]:
                col = f"VKM_PL_{flux_code}{charge}_{d}"
                if col in sub.columns:
                    val += sub[col].sum()
            row[d] = val / 1000
        agg_rows.append(row)

    df_agg = pd.DataFrame(agg_rows)
    df_agg["total"] = sum(df_agg[d] for d in CLASSES_DISTANCE)
    df_agg = df_agg.sort_values("total", ascending=True)

    if labels_mz:
        df_agg["label"] = df_agg["M1"].map(labels_mz)
    else:
        df_agg["label"] = "MZ " + df_agg["M1"].astype(str)

    fig = go.Figure()
    for d_code in reversed(list(CLASSES_DISTANCE.keys())):
        fig.add_trace(go.Bar(
            y=df_agg["label"],
            x=df_agg[d_code],
            name=LABELS_DISTANCE_COURTS[d_code],
            orientation="h",
            marker_color=COULEURS_DISTANCE[d_code],
        ))

    flux_titre = FLUX_LABELS.get(flux_code, flux_code)
    fig.update_layout(
        **LAYOUT_COMMUN,
        barmode="stack",
        title=dict(
            text=f"<b>VKM poids lourds par distance</b> — {flux_titre}",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        xaxis_title="VKM PL (k km/j)",
        legend=dict(
            orientation="h", y=-0.06, x=0.5, xanchor="center",
            font=dict(size=10), bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=220, r=20, t=55, b=55),
        height=max(500, len(df_agg) * 24 + 120),
    )
    _appliquer_axes(fig, ygrid=False)

    return fig


# ---------------------------------------------------------------------------
# Profil de distance comparatif (courbes)
# ---------------------------------------------------------------------------

COULEURS_PROFIL = [
    "#1565C0", "#E53935", "#43A047", "#F57C00", "#7B1FA2",
    "#00838F", "#C62828", "#33691E", "#4527A0", "#BF360C",
    "#0277BD", "#AD1457", "#558B2F", "#6A1B9A", "#E65100",
]


def creer_profil_distance(
    df_raw: pd.DataFrame,
    macrozones: list[int],
    flux_code: str = "T",
    labels_mz: dict | None = None,
) -> go.Figure:
    fig = go.Figure()

    d_labels = list(CLASSES_DISTANCE.values())

    for i, mz in enumerate(macrozones):
        sub = df_raw[df_raw["M1"] == mz]
        vals = []
        for d in CLASSES_DISTANCE:
            val = 0
            for charge in ["C", "V"]:
                col = f"VKM_PL_{flux_code}{charge}_{d}"
                if col in sub.columns:
                    val += sub[col].sum()
            vals.append(val)
        total = sum(vals)
        pcts = [v / total * 100 if total > 0 else 0 for v in vals]

        label = labels_mz.get(mz, f"MZ {mz}") if labels_mz else f"MZ {mz}"
        color = COULEURS_PROFIL[i % len(COULEURS_PROFIL)]
        fig.add_trace(go.Scatter(
            x=d_labels,
            y=pcts,
            mode="lines+markers",
            name=label,
            line=dict(width=2.5, color=color),
            marker=dict(size=7, color=color, line=dict(width=1, color="white")),
        ))

    flux_titre = FLUX_LABELS.get(flux_code, flux_code)
    fig.update_layout(
        **LAYOUT_COMMUN,
        title=dict(
            text=f"<b>Profil de distance PL</b> — {flux_titre}",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        xaxis_title="Classe de distance",
        yaxis_title="% du flux PL",
        legend=dict(
            font=dict(size=10), bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
        ),
        margin=dict(l=60, r=20, t=60, b=50),
        height=420,
    )
    _appliquer_axes(fig)

    return fig


# ---------------------------------------------------------------------------
# Carte camemberts VKM PL par classe de distance
# ---------------------------------------------------------------------------

def creer_carte_camemberts_distance(
    gdf_dissolved,
    df_raw: pd.DataFrame,
    labels_mz: dict | None = None,
    ite_points=None,
    cours_points=None,
) -> go.Figure:
    """Carte avec camemberts proportionnels par macrozone.

    Taille = intensite VKM PL totale, parts = classes de distance D1-D5.
    Les camemberts sont dessines comme des polygones en eventail (wedges)
    sur un fond carto-positron.
    """
    centroids = gdf_dissolved.copy()
    centroids["c_lat"] = centroids.geometry.centroid.y
    centroids["c_lon"] = centroids.geometry.centroid.x

    df_mz = df_raw[df_raw["M1"] != 0]
    dist_keys = list(CLASSES_DISTANCE.keys())

    pie_rows = []
    for _, crow in centroids.iterrows():
        mz = int(crow["MA_ITE"])
        sub = df_mz[df_mz["M1"] == mz]
        if sub.empty:
            continue
        vals = {}
        for d in dist_keys:
            v = 0.0
            for f in ["E", "T", "I"]:
                for c in ["C", "V"]:
                    col = f"VKM_PL_{f}{c}_{d}"
                    if col in sub.columns:
                        v += sub[col].sum()
            vals[d] = v / 1000
        total = sum(vals.values())
        if total <= 0:
            continue
        pie_rows.append({
            "M1": mz,
            "lat": crow["c_lat"],
            "lon": crow["c_lon"],
            "total": total,
            **vals,
        })

    df_p = pd.DataFrame(pie_rows)
    if df_p.empty:
        return go.Figure()

    max_t = df_p["total"].max()
    MIN_R, MAX_R = 0.07, 0.28
    df_p["radius"] = df_p["total"].apply(
        lambda t: MIN_R + (MAX_R - MIN_R) * (t / max_t) ** 0.5
    )

    fig = go.Figure()

    # Fond : contours des macrozones (leger)
    geojson = json.loads(gdf_dissolved.to_json())
    fig.add_trace(go.Choroplethmapbox(
        geojson=geojson,
        locations=list(range(len(gdf_dissolved))),
        z=[0] * len(gdf_dissolved),
        colorscale=[[0, "rgba(200,210,220,0.25)"], [1, "rgba(200,210,220,0.25)"]],
        marker=dict(line=dict(width=1, color="#90A4AE")),
        showscale=False,
        hoverinfo="skip",
    ))

    # Dessiner les parts de camembert par classe de distance
    for d_code in dist_keys:
        all_lats: list = []
        all_lons: list = []

        for _, row in df_p.iterrows():
            if row[d_code] <= 0:
                continue

            cumul = sum(row[dk] for dk in dist_keys[:dist_keys.index(d_code)])
            a_start = cumul / row["total"] * 2 * np.pi - np.pi / 2
            a_end = (cumul + row[d_code]) / row["total"] * 2 * np.pi - np.pi / 2

            r = row["radius"]
            lat_c, lon_c = row["lat"], row["lon"]
            cos_lat = np.cos(np.radians(lat_c))

            n_pts = max(4, int(abs(a_end - a_start) / (2 * np.pi) * 36))
            angles = np.linspace(a_start, a_end, n_pts)

            w_lats = [lat_c]
            w_lons = [lon_c]
            for a in angles:
                w_lats.append(lat_c + r * np.sin(a))
                w_lons.append(lon_c + r * np.cos(a) / cos_lat)
            w_lats.append(lat_c)
            w_lons.append(lon_c)

            all_lats.extend(w_lats + [None])
            all_lons.extend(w_lons + [None])

        if all_lats:
            fig.add_trace(go.Scattermapbox(
                lat=all_lats,
                lon=all_lons,
                mode="lines",
                fill="toself",
                fillcolor=COULEURS_DISTANCE[d_code],
                line=dict(width=0.8, color="white"),
                name=LABELS_DISTANCE_COURTS[d_code],
                hoverinfo="skip",
                showlegend=True,
            ))

    # Points invisibles au centroide pour le hover
    hover_labels = []
    hover_details = []
    for _, row in df_p.iterrows():
        mz = int(row["M1"])
        lbl = labels_mz.get(mz, f"MZ {mz}") if labels_mz else f"MZ {mz}"
        hover_labels.append(lbl)
        parts = [f"VKM PL total : {row['total']:,.0f} k km/j"]
        for dk in dist_keys:
            pct = row[dk] / row["total"] * 100 if row["total"] > 0 else 0
            parts.append(f"  {LABELS_DISTANCE_COURTS[dk]} : {row[dk]:,.0f} ({pct:.1f}%)")
        hover_details.append("<br>".join(parts))

    fig.add_trace(go.Scattermapbox(
        lat=df_p["lat"],
        lon=df_p["lon"],
        mode="markers",
        marker=dict(size=6, color="rgba(0,0,0,0)"),
        text=hover_labels,
        customdata=hover_details,
        hovertemplate="<b>%{text}</b><br>%{customdata}<extra></extra>",
        showlegend=False,
    ))

    _ajouter_marqueurs_ite_cours(fig, ite_points, cours_points)

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center={"lat": 47.0, "lon": 5.0},
            zoom=6.3,
        ),
        font=dict(family=FONT_FAMILY, size=12, color=GRIS_FONCE),
        margin=dict(l=0, r=0, t=50, b=0),
        height=620,
        paper_bgcolor="rgba(0,0,0,0)",
        title=dict(
            text="<b>VKM poids lourds par classe de distance</b>",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        legend=dict(
            yanchor="top", y=0.98, xanchor="left", x=0.01,
            bgcolor="rgba(255,255,255,0.85)",
            font=dict(size=11),
        ),
    )

    return fig


# ---------------------------------------------------------------------------
# Visualisations enrichissement / contexte
# ---------------------------------------------------------------------------

COULEURS_SCORE = ["#E8F5E9", "#A5D6A7", "#66BB6A", "#2E7D32", "#1B5E20"]


def creer_carte_score_composite(
    gdf, metriques, labels_mz, colonne="score_composite",
    ite_points=None, cours_points=None,
):
    """Carte choroplethe du score composite de priorite."""
    gdf_m = gdf.merge(metriques, left_on="MA_ITE", right_on="M1", how="inner")
    gdf_m["label"] = gdf_m["M1"].map(labels_mz).fillna("Macrozone")

    fig = px.choropleth_mapbox(
        gdf_m,
        geojson=json.loads(gdf_m.geometry.to_json()),
        locations=gdf_m.index,
        color=colonne,
        color_continuous_scale=[
            [0, "#E8F5E9"], [0.25, "#A5D6A7"],
            [0.5, "#FFA726"], [0.75, "#EF5350"], [1, "#B71C1C"],
        ],
        hover_name="label",
        hover_data={
            colonne: ":.1f",
            "score_trafic": ":.1f",
            "score_emploi": ":.1f",
            "score_ite": ":.1f",
            "nb_pl_total": ":.0f",
            "emploi_fret": ":.0f",
            "nb_ite": ":.0f",
        },
        mapbox_style="carto-positron",
        center={"lat": 47.0, "lon": 4.5},
        zoom=6.3,
        opacity=0.7,
    )

    _ajouter_marqueurs_ite_cours(fig, ite_points, cours_points)

    fig.update_layout(
        **LAYOUT_COMMUN,
        title=dict(
            text="<b>Score de priorite report modal</b>",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        margin=dict(l=0, r=0, t=50, b=0),
        height=550,
        coloraxis_colorbar=dict(
            title="Score",
            tickfont=dict(size=11),
            len=0.6,
        ),
        legend=dict(
            yanchor="top", y=0.98, xanchor="left", x=0.01,
            bgcolor="rgba(255,255,255,0.85)",
            font=dict(size=11),
        ),
    )
    return fig


def creer_barres_score(metriques, labels_mz, top_n=20,
                       poids_trafic=50, poids_emploi=30, poids_ite=20):
    """Barres horizontales du score composite avec decomposition."""
    total_w = poids_trafic + poids_emploi + poids_ite
    w_t = poids_trafic / total_w if total_w > 0 else 0.5
    w_e = poids_emploi / total_w if total_w > 0 else 0.3
    w_i = poids_ite / total_w if total_w > 0 else 0.2
    pct_t = round(w_t * 100)
    pct_e = round(w_e * 100)
    pct_i = round(w_i * 100)

    df = metriques.nlargest(top_n, "score_composite").sort_values("score_composite")
    df["label"] = df["M1"].map(labels_mz).fillna("MZ")

    fig = go.Figure()

    for col, label_tpl, weight, color in [
        ("score_trafic", "Trafic PL reportable ({pct}%)", w_t, "#1565C0"),
        ("score_emploi", "Emploi fret ({pct}%)", w_e, "#FF8F00"),
        ("score_ite", "ITE ({pct}%)", w_i, "#2E7D32"),
    ]:
        pct = round(weight * 100)
        fig.add_trace(go.Bar(
            y=df["label"],
            x=df[col] * weight,
            name=label_tpl.format(pct=pct),
            orientation="h",
            marker_color=color,
            text=df[col].apply(lambda v: f"{v:.0f}"),
            textposition="inside",
            textfont=dict(size=10, color="white"),
        ))

    fig.update_layout(
        **LAYOUT_COMMUN,
        barmode="stack",
        title=dict(
            text="<b>Score composite de priorite</b> (report modal)",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        legend=dict(
            font=dict(size=11), orientation="h",
            yanchor="bottom", y=1.02, xanchor="center", x=0.5,
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=220, r=30, t=80, b=40),
        height=max(400, top_n * 28 + 120),
    )
    _appliquer_axes(fig, xgrid=True, ygrid=False)
    fig.update_xaxes(title_text="Score composite (0-100)")

    return fig


def creer_barres_secteurs(emploi_detail, code_mz, labels_mz):
    """Barres des emplois par secteur fret pour une macrozone."""
    df = emploi_detail[emploi_detail["M1"] == code_mz].copy()
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            **LAYOUT_COMMUN,
            title="Pas de donnees emploi pour cette macrozone",
        )
        return fig

    df = df.sort_values("emploi", ascending=True)
    nom = labels_mz.get(code_mz, f"MZ {code_mz}")

    colors = px.colors.sequential.Blues_r
    n = len(df)
    bar_colors = [colors[min(i * len(colors) // n, len(colors) - 1)] for i in range(n)]

    fig = go.Figure(go.Bar(
        y=df["label_secteur"],
        x=df["emploi"],
        orientation="h",
        marker_color=bar_colors,
        text=df["emploi"].apply(lambda v: f"{v:,}".replace(",", " ")),
        textposition="outside",
        textfont=dict(size=11),
    ))

    fig.update_layout(
        **LAYOUT_COMMUN,
        title=dict(
            text=f"<b>Emploi fret par secteur</b> — {nom}",
            font=dict(size=14, family=FONT_FAMILY),
        ),
        margin=dict(l=200, r=60, t=60, b=40),
        height=max(350, n * 30 + 120),
    )
    _appliquer_axes(fig, xgrid=True, ygrid=False)
    fig.update_xaxes(title_text="Nombre d'emplois salaries")

    return fig


def creer_radar_macrozone(metriques, code_mz, labels_mz):
    """Radar multi-axes pour une macrozone vs moyenne BFC."""
    row = metriques[metriques["M1"] == code_mz]
    if row.empty:
        fig = go.Figure()
        fig.update_layout(**LAYOUT_COMMUN, title="Macrozone introuvable")
        return fig
    row = row.iloc[0]
    nom = labels_mz.get(code_mz, f"MZ {code_mz}")

    axes = ["pct_pl", "pct_transit_pl", "pct_longue_distance_pl"]
    ax_labels = ["% PL (VKM)", "% Transit PL", "% Long. dist. PL"]
    if "score_composite" in metriques.columns:
        axes.append("score_composite")
        ax_labels.append("Score priorite")
    if "nb_ite" in metriques.columns:
        axes.append("nb_ite")
        ax_labels.append("Nb ITE")

    vals_mz = [row.get(a, 0) for a in axes]
    vals_moy = [metriques[a].mean() for a in axes]

    # Normaliser sur [0, max] pour chaque axe
    maxs = [max(metriques[a].max(), 1) for a in axes]
    vals_mz_n = [v / m * 100 for v, m in zip(vals_mz, maxs)]
    vals_moy_n = [v / m * 100 for v, m in zip(vals_moy, maxs)]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=vals_mz_n + [vals_mz_n[0]],
        theta=ax_labels + [ax_labels[0]],
        fill="toself",
        name=nom,
        line=dict(color=BLEU_PRIMAIRE, width=2),
        fillcolor="rgba(21, 101, 192, 0.2)",
    ))
    fig.add_trace(go.Scatterpolar(
        r=vals_moy_n + [vals_moy_n[0]],
        theta=ax_labels + [ax_labels[0]],
        fill="toself",
        name="Moyenne BFC",
        line=dict(color=GRIS_MOYEN, width=1.5, dash="dash"),
        fillcolor="rgba(90, 106, 122, 0.1)",
    ))

    fig.update_layout(
        **LAYOUT_COMMUN,
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=9)),
            angularaxis=dict(tickfont=dict(size=11)),
        ),
        title=dict(
            text=f"<b>Profil multi-criteres</b> — {nom}",
            font=dict(size=14, family=FONT_FAMILY),
        ),
        legend=dict(font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=60, r=60, t=70, b=40),
        height=420,
    )
    return fig


def creer_tableau_croise_html(metriques, labels_mz, top_n=36):
    """Genere un tableau HTML avec indicateurs visuels (feux) par macrozone."""
    df = metriques.nlargest(top_n, "score_composite").copy()
    df["label"] = df["M1"].map(labels_mz).fillna("MZ")

    def _feu(val, seuils):
        if val >= seuils[2]:
            return '<span style="color:#C62828;font-size:1.3em">&#9679;</span>'
        elif val >= seuils[1]:
            return '<span style="color:#FF8F00;font-size:1.3em">&#9679;</span>'
        else:
            return '<span style="color:#2E7D32;font-size:1.3em">&#9679;</span>'

    q_trafic = metriques["score_trafic"].quantile([0.33, 0.66]).values
    q_emploi = metriques["score_emploi"].quantile([0.33, 0.66]).values
    q_ite = metriques["score_ite"].quantile([0.33, 0.66]).values

    rows_html = []
    for _, r in df.iterrows():
        rows_html.append(
            f"<tr>"
            f"<td style='text-align:left;padding:4px 8px'>{r['label']}</td>"
            f"<td style='text-align:center'>{_feu(r['score_trafic'], [0, q_trafic[0], q_trafic[1]])}</td>"
            f"<td style='text-align:right;padding:0 8px'>{r.get('nb_pl_total', 0):,.0f}</td>"
            f"<td style='text-align:center'>{_feu(r['score_emploi'], [0, q_emploi[0], q_emploi[1]])}</td>"
            f"<td style='text-align:right;padding:0 8px'>{r['emploi_fret']:,}</td>"
            f"<td style='text-align:center'>{_feu(r['score_ite'], [0, q_ite[0], q_ite[1]])}</td>"
            f"<td style='text-align:right;padding:0 8px'>{r['nb_ite']}</td>"
            f"<td style='text-align:right;padding:0 8px;font-weight:bold'>{r['score_composite']:.1f}</td>"
            f"</tr>"
        )

    html = (
        "<table style='width:100%;border-collapse:collapse;font-size:0.9em'>"
        "<thead><tr style='border-bottom:2px solid #1565C0'>"
        "<th style='text-align:left;padding:6px'>Macrozone</th>"
        "<th>Trafic PL</th><th>PL/j</th>"
        "<th>Emploi fret</th><th>Emplois</th>"
        "<th>ITE</th><th>Nb</th>"
        "<th>Score</th>"
        "</tr></thead>"
        "<tbody>" + "".join(rows_html) + "</tbody>"
        "</table>"
    )
    return html


# ---------------------------------------------------------------------------
# Visualisations — Analyse cordon Pagny (report fluvial)
# ---------------------------------------------------------------------------

COULEUR_ENTRANT = "#1565C0"
COULEUR_SORTANT = "#E53935"


def _traces_isochrone_gdf(
    fig: go.Figure,
    gdf,
    fillcolor: str,
    linecolor: str,
    legend_name: str,
) -> None:
    """Ajoute les polygones d'un GeoDataFrame (WGS84) sur la carte Mapbox."""
    if gdf is None or gdf.empty:
        return
    geo = json.loads(gdf.to_json())
    leg_done = False
    for feat in geo.get("features", []):
        g = (feat or {}).get("geometry") or {}
        t, coords = g.get("type"), g.get("coordinates", [])
        rings = []
        if t == "Polygon" and coords:
            rings.append(coords[0])
        elif t == "MultiPolygon":
            for poly in coords:
                if poly:
                    rings.append(poly[0])
        for ring in rings:
            lons = [c[0] for c in ring]
            lats = [c[1] for c in ring]
            fig.add_trace(go.Scattermapbox(
                lon=lons, lat=lats, mode="lines", fill="toself",
                fillcolor=fillcolor, line=dict(width=2, color=linecolor),
                name=legend_name if not leg_done else None,
                showlegend=not leg_done, hoverinfo="skip",
            ))
            leg_done = True


def creer_carte_pagny_isochrone(
    gdf_isochrone,
    df_cordon: pd.DataFrame,
    gdf_dissolved=None,
    ite_points=None,
    cours_points=None,
    mapbox_token: str | None = None,
    aires_chalandise_60m=None,
) -> go.Figure:
    """Carte Mapbox : isochrone Pagny, optionnellement aires 60 min Chalon / Mâcon, cordon.

    ``aires_chalandise_60m`` : liste de dicts ``gdf``, ``label``, ``fill``, ``line``,
    ``centroid`` (lat, lon) — produit par :func:`data_loader.charger_aires_60min_chalon_macon`.

    ``mapbox_token`` (depuis ``MAPBOX_TOKEN`` / ``st.secrets``) requis pour le fond
    ``carto-positron``. Sans token, fond ``open-street-map`` (sans compte).
    """
    fig = go.Figure()
    aires = aires_chalandise_60m or []

    if gdf_dissolved is not None:
        geo_bg = json.loads(gdf_dissolved.to_json())
        fig.add_trace(go.Choroplethmapbox(
            geojson=geo_bg,
            locations=list(range(len(gdf_dissolved))),
            z=[0] * len(gdf_dissolved),
            colorscale=[[0, "rgba(200,210,220,0.2)"], [1, "rgba(200,210,220,0.2)"]],
            marker=dict(line=dict(width=0.8, color="#B0BEC5")),
            showscale=False, hoverinfo="skip",
        ))

    if gdf_isochrone is not None:
        _traces_isochrone_gdf(
            fig, gdf_isochrone,
            "rgba(21,101,192,0.15)", "#1565C0",
            "Isochrone 1h (Pagny)",
        )

    for a in aires:
        g = a.get("gdf")
        if g is None or getattr(g, "empty", True):
            continue
        _traces_isochrone_gdf(
            fig, g,
            a.get("fill", "rgba(0,131,143,0.16)"),
            a.get("line", "#00838F"),
            a.get("label", "Aire 1h"),
        )

    if not df_cordon.empty:
        agg_zone = (
            df_cordon.groupby(["zone_ext", "direction"])
            .agg(pl=("nb_pl_jour", "sum"), dist=("distance_km", "mean"))
            .reset_index()
        )
        # placeholder: no geocoding of external zones, skip scatter for now

    pagny_lat, pagny_lon = 46.97, 5.13
    fig.add_trace(go.Scattermapbox(
        lat=[pagny_lat], lon=[pagny_lon], mode="markers+text",
        marker=dict(size=14, color="#1565C0", symbol="harbor"),
        text=["Pagny"], textposition="top center",
        textfont=dict(size=12, color="#1565C0", family=FONT_FAMILY),
        name="Port de Pagny", showlegend=True,
        hovertemplate="<b>Port de Pagny-le-Chateau</b><br>Plateforme trimodale Aproport<extra></extra>",
    ))

    for a in aires:
        c = a.get("centroid")
        if not c or len(c) < 2:
            continue
        lat, lon = c[0], c[1]
        col = a.get("line", "#00838F")
        lab = a.get("label", "")
        if "Chalon" in lab:
            short = "Chalon"
        elif "Mâcon" in lab or "Macon" in lab:
            short = "Mâcon"
        else:
            short = (lab or "Port")[:20]
        hov = f"<b>{lab}</b><br>Aire 1h chalandise<extra></extra>" if lab else "Aire 1h chalandise<extra></extra>"
        fig.add_trace(go.Scattermapbox(
            lat=[lat], lon=[lon], mode="markers+text",
            marker=dict(size=12, color=col, symbol="harbor"),
            text=[short], textposition="top center",
            textfont=dict(size=11, color=col, family=FONT_FAMILY),
            name=(lab or "Port")[:40], showlegend=True,
            hovertemplate=hov,
        ))

    _ajouter_marqueurs_ite_cours(fig, ite_points, cours_points)

    if mapbox_token:
        mapbox_layout = dict(
            style="carto-positron",
            accesstoken=mapbox_token,
            center={"lat": 47.0, "lon": 4.5},
            zoom=5.8,
        )
    else:
        mapbox_layout = dict(
            style="open-street-map",
            center={"lat": 47.0, "lon": 4.5},
            zoom=5.8,
        )
    fig.update_layout(
        mapbox=mapbox_layout,
        font=dict(family=FONT_FAMILY, size=12, color=GRIS_FONCE),
        margin=dict(l=0, r=0, t=50, b=0), height=550,
        paper_bgcolor="rgba(0,0,0,0)",
        title=dict(
            text="<b>Aires 1h chalandise (Pagny"
            + (", Chalon, Mâcon" if aires else "")
            + ")</b>",
            font=dict(size=15, family=FONT_FAMILY),
        ),
        legend=dict(yanchor="top", y=0.98, xanchor="left", x=0.01,
                    bgcolor="rgba(255,255,255,0.85)", font=dict(size=11)),
    )
    return fig


def creer_barres_cordon_distance(df_cordon: pd.DataFrame) -> go.Figure:
    """Barres empilees PL/jour par classe de distance D2-D5, groupees entrant/sortant."""
    if df_cordon.empty:
        return go.Figure()

    agg = (
        df_cordon.groupby(["classe_distance", "direction"])["nb_pl_jour"]
        .sum().reset_index()
    )
    classes = ["D2", "D3", "D4", "D5"]
    labels = {"D2": "100-200 km", "D3": "200-400 km", "D4": "400-1000 km", "D5": "> 1000 km"}
    colors = {"entrant": COULEUR_ENTRANT, "sortant": COULEUR_SORTANT}

    fig = go.Figure()
    for direction in ["entrant", "sortant"]:
        sub = agg[agg["direction"] == direction]
        vals = [sub.loc[sub["classe_distance"] == c, "nb_pl_jour"].sum() for c in classes]
        fig.add_trace(go.Bar(
            y=[labels.get(c, c) for c in classes], x=vals,
            name=direction.capitalize(), orientation="h",
            marker_color=colors[direction],
            text=[f"{v:,.0f}" for v in vals], textposition="auto",
        ))

    fig.update_layout(
        barmode="group",
        title=dict(text="<b>PL/jour par classe de distance (> 100 km)</b>",
                   font=dict(size=14, family=FONT_FAMILY)),
        xaxis_title="PL / jour",
        **LAYOUT_COMMUN,
        margin=dict(l=120, r=20, t=50, b=50), height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    _appliquer_axes(fig)
    return fig


def _dep_libelle_cordon(dep) -> str:
    """Département en 2 chiffres, ou '?' si absent, non numérique ou 0 (code invalide)."""
    if dep is None or (isinstance(dep, (float, int)) and pd.isna(dep)):
        return "?"
    try:
        d = int(float(dep))
    except (TypeError, ValueError):
        s = str(dep).strip()
        return s[:4] if s else "?"
    if d <= 0:
        return "?"
    return f"{d:02d}"


def _insee_commune_plausible(v) -> bool:
    """True si v ressemble a un code commune Insee (hors 0 / sentinelle)."""
    if v is None or (isinstance(v, (float, int)) and pd.isna(v)):
        return False
    try:
        c = int(float(v))
    except (TypeError, ValueError):
        return False
    if c <= 0:
        return False
    # Métropole 01001+ ; outre-mer 971xx (5 chiffres) ; borne large pour le ref. Insee
    return 1000 < c < 1_000_000


def _tronque(s: str, n: int = 58) -> str:
    s = s.strip()
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def _libelle_contexte_cordon(
    row: dict, zone_id: int, labels_mz: dict | None
) -> str:
    """Texte d’axe : priorite au reseau ITE (shapefile), puis Insee, puis distance.

    L’ID zone OPSAM (zone_id) n’est pas affiché ici : il sert d’identifiant technique
    (infobulle). Le lookup ne contient pas le nom des communes, seulement le code
    Insee (com_ext) et M1 (mz_ext). Les zones non documentees (0 dans le ref.)
    s’appuient sur la distance modelisee.
    """
    z = int(zone_id)
    dep_s = _dep_libelle_cordon(row.get("dep"))
    dkm = row.get("dkm")
    if dkm is not None and pd.notna(dkm):
        try:
            dkm = float(dkm)
        except (TypeError, ValueError):
            dkm = None
    else:
        dkm = None
    cls = row.get("classe")
    com = row.get("com")
    mz = row.get("mz")

    # 1) Macrozone ITE (M1) : libellé communes / dép. (shapefile)
    if labels_mz and mz is not None and pd.notna(mz):
        try:
            mzi = int(float(mz))
        except (TypeError, ValueError):
            mzi = 0
        if mzi > 0:
            t = labels_mz.get(mzi) or f"Réseau ITE — zone {mzi}"
            t = _tronque(t, 60)
            if dep_s != "?" and "dpt" not in t.lower():
                t = f"{t} (dpt {dep_s})"
            return t

    # 2) Code Insee (donnée réf. ; pas le nom communal dans ce CSV)
    if _insee_commune_plausible(com):
        ci = int(float(com))
        if dep_s != "?":
            return f"Commune (insee n°{ci:05d}, dpt {dep_s})"
        return f"Commune (insee n°{ci:05d})"

    # 3) Distance / classe (zones « vides » dans le lookup, ex. ID 4215)
    if dkm is not None and cls is not None and str(cls).strip() != "":
        cname = CLASSES_DISTANCE.get(str(cls), str(cls))
        return _tronque(f"Au-delà du cordon — {cname} (≈{dkm:.0f} km)", 64)
    if dkm is not None:
        return f"Au-delà du cordon — longue distance (≈{dkm:.0f} km)"
    if cls is not None and str(cls).strip() != "":
        cname = CLASSES_DISTANCE.get(str(cls), str(cls))
        return f"Au-delà du cordon — {cname}"

    if dep_s != "?":
        return f"Hinterland (dpt {dep_s}, distance modélisée)"
    return "Hinterland (au-delà du cordon, distance modélisée)"


def _dedoublonner_libelles_cordon(
    base_map: dict, zone_to_dkm: dict
) -> dict:
    """Si deux zones ont le même texte, différencier par distance (sinon indice)."""
    inv: dict[str, list[int]] = {}
    for z, lab in base_map.items():
        inv.setdefault(lab, []).append(z)
    out = {}
    for lab, zlist in inv.items():
        if len(zlist) == 1:
            out[zlist[0]] = lab
            continue
        for i, z in enumerate(sorted(zlist), start=1):
            dkm = zone_to_dkm.get(z)
            if dkm is not None and pd.notna(dkm):
                suf = f" — pôle {i} (≈{float(dkm):.0f} km)"
            else:
                suf = f" — pôle {i}"
            out[z] = _tronque(lab + suf, 70)
    return out


def creer_barres_cordon_origines(
    df_cordon: pd.DataFrame, top_n: int = 15
) -> go.Figure:
    """Top N origines/destinations : libellé métier (ITE, insee, distance) ; ID zone en infobulle."""
    if df_cordon.empty:
        return go.Figure()

    try:
        labels_mz = generer_labels_macrozones(CHEMIN_SHP_DEFAUT)
    except Exception:
        labels_mz = {}

    agg_dict: dict = {
        "pl": ("nb_pl_jour", "sum"),
        "dep": ("dep_ext", "first"),
        "mz": ("mz_ext", "first"),
    }
    if "com_ext" in df_cordon.columns:
        agg_dict["com"] = ("com_ext", "first")
    if "reg_ext" in df_cordon.columns:
        agg_dict["reg"] = ("reg_ext", "first")
    if "distance_km" in df_cordon.columns:
        agg_dict["dkm"] = ("distance_km", "mean")
    if "classe_distance" in df_cordon.columns:
        agg_dict["classe"] = ("classe_distance", "first")

    agg = (
        df_cordon.groupby(["zone_ext", "direction"], dropna=False)
        .agg(**agg_dict)
        .reset_index()
    )
    top_zones = (
        agg.groupby("zone_ext")["pl"].sum()
        .nlargest(top_n).index
    )
    agg = agg[agg["zone_ext"].isin(top_zones)]

    by_zone = agg.drop_duplicates("zone_ext", keep="first")
    base_map: dict[int, str] = {}
    zone_dkm: dict[int, float | None] = {}
    for _, row in by_zone.iterrows():
        z = int(row["zone_ext"])
        r = row.to_dict()
        base_map[z] = _libelle_contexte_cordon(r, z, labels_mz)
        d = r.get("dkm")
        try:
            zone_dkm[z] = float(d) if d is not None and pd.notna(d) else None
        except (TypeError, ValueError):
            zone_dkm[z] = None
    label_map = _dedoublonner_libelles_cordon(base_map, zone_dkm)

    agg["label"] = agg["zone_ext"].map(label_map)
    agg = agg.sort_values("pl", ascending=True)

    max_lbl = int(agg["label"].str.len().max()) if len(agg) else 20
    margin_l = min(460, max(200, 7 * max(24, min(max_lbl, 72)) // 10))

    # Infobulle : rappel de l'identifiant zone du modèle (hors texte d'axe)
    hov = (
        "%{y}<br>"
        "%{x:,.0f} PL/j<br>"
        "Identifiant zone (modèle OPSAM) : %{customdata}"
        "<extra></extra>"
    )

    fig = go.Figure()
    for direction in ["entrant", "sortant"]:
        sub = agg[agg["direction"] == direction]
        fig.add_trace(go.Bar(
            y=sub["label"],
            x=sub["pl"],
            name=direction.capitalize(),
            orientation="h",
            customdata=sub["zone_ext"],
            marker_color=COULEUR_ENTRANT if direction == "entrant" else COULEUR_SORTANT,
            hovertemplate=hov,
        ))

    fig.update_layout(
        barmode="stack",
        title=dict(
            text=(
                f"<b>Top {top_n} origines / destinations</b> "
                "<sup>(l’axe : pôle ITE, commune Insee ou tranche de distance — "
                "l’ID technique du modèle est au survol)</sup>"
            ),
            font=dict(size=14, family=FONT_FAMILY),
        ),
        xaxis_title="PL / jour",
        **LAYOUT_COMMUN,
        yaxis=dict(tickfont=dict(size=10)),
        margin=dict(l=margin_l, r=20, t=58, b=50), height=max(400, top_n * 32),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    _appliquer_axes(fig)
    return fig


def creer_donut_flux_pagny(df_cordon: pd.DataFrame) -> go.Figure:
    """Donut echange / transit / interne pour les flux cordon."""
    if df_cordon.empty:
        return go.Figure()

    agg = df_cordon.groupby("flux_type")["nb_pl_jour"].sum().reset_index()
    type_map = {"E": "Echange", "T": "Transit", "I": "Interne"}
    agg["label"] = agg["flux_type"].map(type_map)
    colors = [COULEURS_FLUX.get(l, "#999") for l in agg["label"]]

    fig = go.Figure(go.Pie(
        labels=agg["label"], values=agg["nb_pl_jour"],
        hole=0.5, marker=dict(colors=colors),
        textinfo="label+percent", textfont=dict(size=12),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} PL/j<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="<b>Repartition par type de flux</b>",
                   font=dict(size=14, family=FONT_FAMILY)),
        **LAYOUT_COMMUN,
        margin=dict(l=20, r=20, t=60, b=20), height=320,
        showlegend=False,
    )
    return fig


def creer_barres_bassins_ventile(
    df_cordon: pd.DataFrame,
    bassins: list,
    coeffs: dict[str, float],
) -> go.Figure:
    """Barres empilées : pour chaque bassin (zone_ext OPSAM + id), PL/j ventilés par distance."""
    if df_cordon.empty:
        return go.Figure()

    classes = ["D2", "D3", "D4", "D5"]
    labels = {"D2": "100-200 km", "D3": "200-400 km", "D4": "400-1000 km", "D5": "> 1000 km"}

    fig = go.Figure()
    n_traces = 0
    for b in bassins:
        if not isinstance(b, dict):
            continue
        bid = b.get("id", "")
        zid = int(b.get("zone_opsam") or 0)
        if zid <= 0:
            continue
        nom = b.get("nom", bid)
        coeff = float(coeffs.get(bid, b.get("coeff", 0) or 0))
        col = b.get("color", "#5C6BC0")
        sub = df_cordon[df_cordon["zone_ext"] == zid]
        vals = []
        for c in classes:
            raw = sub.loc[sub["classe_distance"] == c, "nb_pl_jour"].sum()
            vals.append(float(raw) * coeff)
        if not any(vals):
            continue
        n_traces += 1
        pct = coeff * 100
        fig.add_trace(go.Bar(
            y=[labels[cc] for cc in classes], x=vals,
            name=f"{nom} ({pct:.0f}%)",
            orientation="h",
            marker_color=col,
            text=[f"{v:,.0f}" for v in vals], textposition="auto",
        ))

    if n_traces == 0:
        return go.Figure()

    fig.update_layout(
        barmode="stack",
        title=dict(
            text="<b>Corridor Saone-Rhone — PL/j ventiles par distance (bassins actifs)</b>",
            font=dict(size=14, family=FONT_FAMILY),
        ),
        xaxis_title="PL / jour (estime)",
        **LAYOUT_COMMUN,
        margin=dict(l=120, r=20, t=50, b=50), height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    _appliquer_axes(fig)
    return fig


def creer_barres_fos_sete(
    df_cordon: pd.DataFrame,
    coeff_fos: float = 0.10,
    coeff_sete: float = 0.05,
) -> go.Figure:
    """Rétrocompat : Fos + Sète via :func:`creer_barres_bassins_ventile`."""
    bass = [
        {"id": "fos", "nom": "ZIP Fos", "zone_opsam": 5125, "color": "#1565C0"},
        {"id": "sete", "nom": "Bassin Sete", "zone_opsam": 5124, "color": "#F57C00"},
    ]
    return creer_barres_bassins_ventile(
        df_cordon, bass, {"fos": coeff_fos, "sete": coeff_sete}
    )


def creer_contexte_fluvial() -> go.Figure:
    """Graphique de synthese part modale BFC (routier/ferro/fluvial)."""
    modes = ["Routier", "Ferroviaire", "Fluvial"]
    tkm = [14_079_045_151, 917_780_019, 76_238_269]
    total = sum(tkm)
    pcts = [v / total * 100 for v in tkm]
    colors_ctx = ["#546E7A", "#F57C00", BLEU_PRIMAIRE]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=modes, y=pcts,
        marker_color=colors_ctx,
        text=[f"{p:.1f}%" for p in pcts],
        textposition="outside",
        textfont=dict(size=13, family=FONT_FAMILY),
    ))
    fig.update_layout(
        title=dict(
            text="<b>Part modale fret en BFC</b> (t-km, 2019 — source SDES)",
            font=dict(size=14, family=FONT_FAMILY),
        ),
        yaxis_title="% des t-km",
        **LAYOUT_COMMUN,
        margin=dict(l=60, r=20, t=60, b=50), height=350,
    )
    _appliquer_axes(fig, xgrid=False)
    return fig
