"""
pdf_export.py — Génération de rapports PDF pour les macrozones OPSAM.

Export d'images Plotly via kaleido puis insertion dans un PDF via reportlab.
Style aligné sur le modèle départemental (analyse_flux_complet_dep_sankey.py).
"""

from pathlib import Path
from io import BytesIO
import logging

import plotly.graph_objects as go

from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image as RLImage,
    Table,
    TableStyle,
    PageBreak,
)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

try:
    from pypdf import PdfWriter, PdfReader
except ImportError:
    try:
        from PyPDF2 import PdfWriter, PdfReader
    except ImportError:
        PdfWriter, PdfReader = None, None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_log = logging.getLogger(__name__)


def _prepare_fig_for_raster_export(fig) -> "go.Figure | None":
    """Copie la figure et force polices + fonds lisibles pour Chromium (ex. cloud Linux)."""
    if fig is None:
        return None
    try:
        if not getattr(fig, "data", None) or len(fig.data) == 0:
            return None
    except Exception:
        return None
    try:
        out = go.Figure(fig)
    except Exception:
        out = fig  # type: ignore[assignment]
    try:
        out.update_layout(
            font=dict(
                family="Arial, Helvetica, Liberation Sans, DejaVu Sans, sans-serif",
            ),
            paper_bgcolor="white",
            plot_bgcolor="white",
        )
    except Exception:
        pass
    return out


def _fig_to_png_bytes(fig, width=1600, height=900, scale=3) -> bytes | None:
    """Exporte une figure Plotly en PNG via kaleido, avec repli (taille / scale) sur environnements contraints (cloud).

    L'ancien mode (une seule tentative, *scale* élevé) provoquait souvent l'échec silencieux
    (mémoire Chromium) → pages PDF vides malgré titres/pieds.
    """
    fig_e = _prepare_fig_for_raster_export(fig)
    if fig_e is None:
        return None

    # Largeur/hauteur demandée en premier réduit, puis repli — *scale* bas en priorité (évite OOM).
    w0, h0, s0 = max(400, int(width)), max(300, int(height)), max(1, min(3, int(scale)))
    attempts: list[tuple[int, int, int]] = [
        (min(w0, 1000), min(h0, 650), 1),
        (800, 500, 1),
        (1200, 720, 1),
        (w0, h0, 1),
        (min(w0, 1400), min(h0, 900), 2),
    ]
    last_err: Exception | None = None
    for w, h, sc in attempts:
        try:
            b = fig_e.to_image(
                format="png",
                width=w,
                height=h,
                scale=sc,
                engine="kaleido",
            )
            if b and len(b) > 200:
                return b
        except Exception as e:  # noqa: BLE001
            last_err = e
        try:
            b = fig_e.to_image(
                format="png",
                width=w,
                height=h,
                scale=sc,
            )
            if b and len(b) > 200:
                return b
        except Exception as e:  # noqa: BLE001
            last_err = e
    if last_err is not None:
        _log.warning("Export PNG (kaleido) echec : %s", last_err, exc_info=False)
        print(f"  Erreur export image (tentatives epuisées) : {last_err}")
    return None


def _make_styles():
    """Styles ReportLab alignés sur le modèle départemental."""
    base = getSampleStyleSheet()

    titre = ParagraphStyle(
        "TitreMZ", parent=base["Heading1"],
        fontSize=15, spaceAfter=6, alignment=1,
        wordWrap="CJK",
    )
    sous_titre = ParagraphStyle(
        "SousTitreMZ", parent=base["Heading2"],
        fontSize=12, spaceAfter=8, alignment=1,
    )
    info = ParagraphStyle(
        "InfoMZ", parent=base["Normal"],
        fontSize=9, spaceAfter=3, spaceBefore=8, alignment=1,
    )
    footer = ParagraphStyle(
        "FooterMZ", parent=base["Normal"],
        fontSize=9, spaceAfter=0, spaceBefore=6, alignment=1,
    )
    return titre, sous_titre, info, footer


# ---------------------------------------------------------------------------
# Rapport individuel par macrozone
# ---------------------------------------------------------------------------

def generer_rapport_macrozone(
    code_mz: int,
    nom_mz: str,
    fig_sankey,
    fig_donut,
    fig_distance,
    fig_pl_detail,
    repartition: dict,
    chemin_sortie: str | Path | None,
) -> bytes | None:
    """Rapport PDF individuel — layout Sankey+Donut identique au modèle dep.

    * ``chemin_sortie`` fourni : écrit le fichier sur le disque (hors conteneur read-only), retourne ``None``.
    * ``chemin_sortie`` ``None`` : génère en **mémoire** (téléchargement Streamlit) et retourne les **bytes** du PDF.
    """
    use_mem = chemin_sortie is None
    if not use_mem:
        chemin_sortie = Path(chemin_sortie)  # type: ignore[assignment]
        chemin_sortie.parent.mkdir(parents=True, exist_ok=True)
    buf = BytesIO() if use_mem else None
    dest = buf if use_mem else str(chemin_sortie)

    titre_style, sous_titre_style, info_style, footer_style = _make_styles()

    doc = SimpleDocTemplate(
        dest,
        pagesize=landscape(A4),
        title=f"Macrozone {code_mz} — Analyse trafic",
        author="Outil OPSAM — Atmo BFC",
        leftMargin=0.15 * inch,
        rightMargin=0.2 * inch,
        topMargin=0.3 * inch,
        bottomMargin=0.25 * inch,
    )

    story = []

    # ── Page 1 : Sankey + Donut (layout table identique modèle dep) ──
    story.append(Paragraph(
        f"Analyse du trafic routier — {nom_mz}", titre_style,
    ))

    png_sankey = _fig_to_png_bytes(fig_sankey, width=1600, height=950, scale=2)
    png_donut = _fig_to_png_bytes(fig_donut, width=450, height=350, scale=2)

    if png_sankey and png_donut:
        img_sankey = RLImage(
            BytesIO(png_sankey), width=8.2 * inch, height=4.8 * inch,
        )
        img_donut = RLImage(
            BytesIO(png_donut), width=2.2 * inch, height=1.8 * inch,
        )

        data = [
            ["Flux de trafic par infrastructure", "Répartition VL/PL"],
            [img_sankey, img_donut],
        ]
        tbl = Table(data, colWidths=[8.3 * inch, 2.2 * inch])
        tbl.setStyle(TableStyle([
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("ALIGN", (1, 0), (1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 11),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("TOPPADDING", (0, 1), (-1, 1), 3),
            ("LEFTPADDING", (0, 1), (0, 1), 0),
            ("RIGHTPADDING", (0, 1), (0, 1), 3),
            ("LEFTPADDING", (1, 1), (1, 1), 3),
            ("RIGHTPADDING", (1, 1), (1, 1), 0),
        ]))
        story.append(tbl)
    elif png_sankey:
        story.append(RLImage(
            BytesIO(png_sankey), width=10.6 * inch, height=5.0 * inch,
        ))

    # Résumé chiffré
    vl_k = repartition["vkm_vl"] / 1000
    pl_k = repartition["vkm_pl"] / 1000
    tot_k = repartition["total_vkm"] / 1000
    resume = (
        f"<b>Résumé :</b> Volume total de {tot_k:.0f} milliers km/jour | "
        f"VL : {repartition['pct_vl']:.1f}% ({vl_k:.0f} milliers km/jour) | "
        f"PL : {repartition['pct_pl']:.1f}% ({pl_k:.0f} milliers km/jour)"
    )
    story.append(Paragraph(resume, info_style))
    story.append(Paragraph(
        f"<b>{nom_mz}</b> | Répartition des flux de trafic | "
        f"<b>Outil OPSAM — Atmo BFC</b>",
        footer_style,
    ))

    # ── Page 2 : Distance + PL ──
    story.append(PageBreak())
    story.append(Paragraph(
        f"Analyse par classe de distance — {nom_mz}", titre_style,
    ))

    png_dist = _fig_to_png_bytes(fig_distance, width=1600, height=700, scale=2)
    if png_dist:
        story.append(RLImage(
            BytesIO(png_dist), width=10.6 * inch, height=4.3 * inch,
        ))

    if fig_pl_detail is not None:
        png_pl = _fig_to_png_bytes(fig_pl_detail, width=1400, height=550, scale=2)
        if png_pl:
            story.append(Spacer(1, 6))
            story.append(RLImage(
                BytesIO(png_pl), width=9.5 * inch, height=3.4 * inch,
            ))

    story.append(Paragraph(
        f"<b>{nom_mz}</b> | Répartition par distance | "
        f"<b>Outil OPSAM — Atmo BFC</b>",
        footer_style,
    ))

    try:
        doc.build(story)
        if use_mem and buf is not None:
            return buf.getvalue()
        if not use_mem and chemin_sortie is not None:
            print(f"  PDF macrozone exporté : {Path(chemin_sortie).name}")
    except Exception as e:
        print(f"  Erreur PDF macrozone : {e}")
        raise
    return None


# ---------------------------------------------------------------------------
# Rapport global de synthèse
# ---------------------------------------------------------------------------

def generer_rapport_global(
    fig_carte,
    fig_heatmap,
    fig_scatter,
    tableau_html: str,
    chemin_sortie: str | Path | None,
) -> bytes | None:
    """PDF de synthèse globale — haute résolution.

    * ``chemin_sortie`` fourni : écrit le fichier, retourne ``None``.
    * ``None`` : retourne les **bytes** (téléchargement côté navigateur).
    """
    use_mem = chemin_sortie is None
    if not use_mem:
        p = Path(chemin_sortie)  # type: ignore[arg-type]
        p.parent.mkdir(parents=True, exist_ok=True)
    buf = BytesIO() if use_mem else None
    dest = buf if use_mem else str(chemin_sortie)

    titre_style, sous_titre_style, info_style, footer_style = _make_styles()

    doc = SimpleDocTemplate(
        dest,
        pagesize=landscape(A4),
        title="Synthèse macrozones OPSAM",
        author="Outil OPSAM — Atmo BFC",
        leftMargin=0.4 * inch,
        rightMargin=0.4 * inch,
        topMargin=0.4 * inch,
        bottomMargin=0.3 * inch,
    )

    story = []

    # Page 1 : Carte
    story.append(Paragraph(
        "Synthèse des macrozones — Analyse du trafic routier", titre_style,
    ))
    png_carte = _fig_to_png_bytes(fig_carte, width=1600, height=900, scale=3)
    if png_carte:
        story.append(RLImage(
            BytesIO(png_carte), width=10.6 * inch, height=5.5 * inch,
        ))
    story.append(Paragraph(
        "Répartition des flux de trafic | <b>Outil OPSAM — Atmo BFC</b>",
        footer_style,
    ))

    # Page 2 : Heatmap
    story.append(PageBreak())
    story.append(Paragraph("Comparaison des macrozones", titre_style))
    png_heatmap = _fig_to_png_bytes(fig_heatmap, width=1400, height=1000, scale=2)
    if png_heatmap:
        story.append(RLImage(
            BytesIO(png_heatmap), width=10 * inch, height=6 * inch,
        ))
    story.append(Paragraph(
        "Indicateurs clés par macrozone | <b>Outil OPSAM — Atmo BFC</b>",
        footer_style,
    ))

    # Page 3 : Scatter
    story.append(PageBreak())
    story.append(Paragraph("Transit vs Poids lourds", titre_style))
    png_scatter = _fig_to_png_bytes(fig_scatter, width=1400, height=800, scale=2)
    if png_scatter:
        story.append(RLImage(
            BytesIO(png_scatter), width=10 * inch, height=5.5 * inch,
        ))
    story.append(Paragraph(
        "% Transit vs % PL par macrozone | <b>Outil OPSAM — Atmo BFC</b>",
        footer_style,
    ))

    try:
        doc.build(story)
        if use_mem and buf is not None:
            return buf.getvalue()
        if not use_mem and chemin_sortie is not None:
            print(f"  PDF global exporté : {Path(chemin_sortie).name}")
    except Exception as e:
        print(f"  Erreur PDF global : {e}")
        raise
    return None


# ---------------------------------------------------------------------------
# Fusion de PDFs individuels en un rapport complet
# ---------------------------------------------------------------------------

def fusionner_pdfs(chemins_pdf: list[Path], chemin_sortie: Path):
    """Fusionne plusieurs PDF en un seul."""
    if not PdfWriter or not PdfReader:
        print("  pypdf/PyPDF2 non disponible — fusion impossible")
        return

    writer = PdfWriter()
    for chemin in chemins_pdf:
        if chemin.exists():
            reader = PdfReader(str(chemin))
            for page in reader.pages:
                writer.add_page(page)

    chemin_sortie.parent.mkdir(parents=True, exist_ok=True)
    with open(chemin_sortie, "wb") as f:
        writer.write(f)
    print(f"  PDF fusionné : {chemin_sortie.name} ({len(writer.pages)} pages)")


def fusionner_pdfs_bytes(pdf_bytes_list: list[bytes]) -> bytes:
    """Fusionne des PDF en **mémoire** (Streamlit, pas d’écriture sur le dépôt / Git)."""
    if not PdfWriter or not PdfReader:
        raise RuntimeError("pypdf / PyPDF2 requis pour la fusion des PDF")
    w = PdfWriter()
    for raw in pdf_bytes_list:
        r = PdfReader(BytesIO(raw))
        for page in r.pages:
            w.add_page(page)
    out = BytesIO()
    w.write(out)
    return out.getvalue()
