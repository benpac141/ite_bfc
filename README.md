# Dashboard ITE / macrozones OPSAM — BFC (Streamlit)

Application **Streamlit** d’analyse des macrozones et du **report fluvial** (cordon Pagny, ventilation, contexte cartographique).  
Dépôt cible : [github.com/benpac141/ite_bfc](https://github.com/benpac141/ite_bfc).

## Prérequis (local)

- Python **3.11** (aligné sur `runtime.txt` pour le cloud)
- Fichier principal : `app.py`

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt
streamlit run app.py
```

## Premier envoi sur GitHub (dépôt vide)

Le dépôt distant peut être vide. À partir du dossier qui contiendra **la racine** du projet (fichier `app.py` à la racine) :

```bash
git init
git add .
git commit -m "Import initial — dashboard Streamlit macrozones / report fluvial"
git branch -M main
git remote add origin https://github.com/benpac141/ite_bfc.git
git push -u origin main
```

Si le projet est encore uniquement sur le réseau interne, **copiez** tout le contenu de ce répertoire (`MACROZONE/`) vers votre clone local de `ite_bfc`, puis exécutez les commandes ci-dessus.

## Déploiement — Streamlit Community Cloud

1. Connectez le compte GitHub sur [share.streamlit.io](https://share.streamlit.io).
2. **New app** : dépôt `benpac141/ite_bfc`, branche `main`, **Main file** : `app.py`.
3. **Advanced settings** → *Python version* : laisser l’app utiliser `runtime.txt` (3.11).
4. **Secrets** (interface Streamlit) : reprenez le modèle `.streamlit/secrets.toml.example` et créez un secret `MAPBOX_TOKEN` si vous voulez le fond Mapbox *positron* ; sans secret, l’app utilise le fond OSM (comportement déjà géré dans `app.py`).

Exemple (format TOML dans l’UI Secrets) :

```toml
MAPBOX_TOKEN = "pk.votre_cle_public_mapbox"
```

5. **Variables d’environnement** (recommandé pour le cloud) : le serveur n’a **ni** `U:\` **ni** lecteur réseau du NAS. Pointez vers des chemins **dans le dépôt** après clone, par ex. :

| Variable | Exemple de valeur (à adapter) |
|----------|--------------------------------|
| `MACROZONE_CSV` | `data/macrozone_test_ITE.csv` |
| `MACROZONE_SHP` | `data/opsam_zonage_metazone_ite_serm.shp` (répertoire requis) |
| `MACROZONE_PAGNY_CORDON_CSV` | `data/bundle_publication/outputs/pagny_cordon_flows.csv` |
| `MACROZONE_PAGNY_VENT_JSON` | `data/bundle_publication/outputs/pagny_ventilation.json` |
| `PAGNY_REPORT` | `data/bundle_publication/PAGNY_REPORT` |
| `MACROZONE_LOOKUP_CSV` | `data/bundle_publication/lookup/lookup_dep_com_epci_macrozone.csv` |
| `MACROZONE_PAGNY_ISO_GEOJSON` | `data/bundle_publication/PAGNY_REPORT/Pagny_aire_60min.geojson` |
| `MACROZONE_AIRES_60M_CHALON_MACON_DIR` | `data/bundle_publication/geojson/aire_60min_chalon_macon` |

Préparez d’abord le dossier **`data/bundle_publication/`** (script `python scripts/rappatrier_publication.py` sur une machine avec accès aux sources), copiez-y les **sorties** de `prepare_pagny.py` (`pagny_cordon_flows.csv`, `pagny_ventilation.json`) dans `outputs/`, puis **validez la taille** des fichiers avant de les committer (éviter de pousser des ressources trop lourdes : utiliser [Git LFS](https://git-lfs.com) si nécessaire pour des shapefiles volumineux).

## Fichiers utiles

| Fichier | Rôle |
|--------|------|
| `requirements.txt` | Dépendances pip (Cloud) |
| `runtime.txt` | Version Python côté Streamlit Cloud |
| `app.py` | Point d’entrée (à renseigner dans l’UI Cloud) |
| `.streamlit/config.toml` | Thème / options |
| `.streamlit/secrets.toml.example` | Modèle pour *Secrets* (Mapbox) |
| `data/bundle_publication/README.md` | Détails sur le bundle et les variables |
| `data/README_CORDON.md` | Méthodo cordon / PAGNY_REPORT |
| `scripts/rappatrier_publication.py` | Copie des fichiers « légers » pour le déploiement |

## Limites

- Le **précalcul lourd** (`prepare_pagny.py` sur les matrices PL) se fait en **hors** cloud ; seuls les **CSV/JSON** produits sont embarqués pour l’affichage.
- Vérifiez les **limites de taille** du dépôt et du déploiement Streamlit (fichiers volumineux via Git LFS ou hébergement externe + URL si vous l’intégrez plus tard).
