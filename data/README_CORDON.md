# Cordon fluvial — extension vallée du Rhône (Chalon, Mâcon, Pagny)

## Fichiers

| Fichier | Rôle |
|---------|------|
| `cordon_rhone_noeuds.json` | Métadonnées des nœuds et rappels sur le double comptage. |
| `zones_noeud_chalon.example.csv` | Exemple d'ID de zones OPSAM (1 colonne `zone_id`) — à remplacer par la liste DREAL. |
| `zones_noeud_macon.example.csv` | Idem. |
| `ventilation_zones_chalon_macon.example.json` | Exemple d'IDs **agrégat** OPSAM pour l'onglet *Bassins* (dép. 71) : copier en `ventilation_zones_chalon_macon.json`. |

## Méthodologie (alignée sur le cordon Pagny)

1. Obtenir pour chaque nœud la **liste d'entiers** `ID_ZONAGE` utilisée dans le modèle (fichier type Excel, comme *liste_zones_pagny60min*).
2. Croiser avec `lookup_opsam_ite/lookup_dep_com_epci_macrozone.csv` si besoin de vérification spatiale.
3. Pour l'**agrégat OPSAM** à utiliser en `zone_ext` côté visualisation *bassins* (Fos, Sète, etc.), lire l'`ID_ZONAGE` ou zone agrégée cible dans le lookup — renseigner `zone_opsam` dans `pagny_ventilation.json` / `bassins_ventilation` (pour Chalon / Mâcon : voir `ventilation_zones_chalon_macon.json` ou variables d'environnement `BASSIN_VENTIL_ZONE_CHALON` / `BASSIN_VENTIL_ZONE_MACON`).
4. Regénérer le CSV cordon : `python prepare_pagny.py` (ou script dérivé) ; comparer totaux avec `python scripts/compare_cordon_csv.py baseline.csv new.csv` avant d'écraser la version de référence.

## Analyse matricielle multi-ports (`prepare_pagny.py`)

1. **Pagny** : toujours via `liste_zones_pagny60min_distincation_paca.xlsx` (feuille `zone_pagny`).
2. **Chalon** : déposer `liste_zones_chalon60min_distincation_paca.xlsx` dans `PAGNY_REPORT` (réf. DREAL ; repli : `liste_zones_chalon_60min.xlsx`), **ou** remplir [`zones_chalon_60min.csv`](zones_chalon_60min.csv) (1re colonne = ID zone OPSAM).
3. **Mâcon** : idem avec `liste_zones_macon60min_distincation_paca.xlsx` ou repli `liste_zones_macon_60min.xlsx`, ou [`zones_macon_60min.csv`](zones_macon_60min.csv).

Lancer `python prepare_pagny.py` : le CSV `pagny_cordon_flows.csv` contient une colonne **`cordon`** (`pagny`, `chalon`, `macon`). L'app **Report fluvial** filtre avec le sélecteur « Cordon ».

## Géométries — aires 1h chalandise Chalon / Mâcon

Les **GeoJSON** placés dans le dossier (par défaut) :

- `U:\21_MOBILITE\21.4_PROJETS\DREAL\2025-2026_MISSION_DREAL\2_INPUT\DATA\PAGNY_REPORT\aire_60min_chalon_macon\`

sont chargés automatiquement et superposés à l'isochrone Pagny sur la carte *Report fluvial → Vue cordon* (fichiers `*.geojson`). Le nom de fichier sert à choisir la légende / couleurs (présence de `chalon` ou `macon` dans le nom, insensible à la casse). Miroir possible sur le NAS : même sous-arborescence sous `PAGNY_REPORT\aire_60min_chalon_macon\`.

- `MACROZONE_AIRES_60M_CHALON_MACON_DIR` — remplace le dossier par défaut (autre disque, dépôt, etc.).

## Variables d'environnement utiles (sécurité / baselines)

- `PAGNY_CORDON_CSV` — nom du CSV produit (ex. `output_dev/pagny_cordon_flows.csv` pour tests).
- `PAGNY_VENTILATION_JSON` — chemin du JSON de ventilation.
- `MACROZONE_PAGNY_CORDON_CSV` — chemin côté **app** Streamlit pour lire le CSV d'affichage.
- `BASSIN_VENTIL_ZONE_CHALON` / `BASSIN_VENTIL_ZONE_MACON` — (optionnel) surchargent `zone_opsam` pour l'onglet *Bassins* (dép. 71) s'ils ne sont ni dans le JSON ni lus depuis Excel.
- `BASSIN_VENTIL_SHEET_CHALON` / `BASSIN_VENTIL_SHEET_MACON` — noms d'onglets (défauts : `zone_chalon`, `zone_macon`) pour y lire l'ID d'agrégat.
- `PAGNY_REPORT` — pointe le dossier des Excel (ex. `U:\21_MOBILITE\...\PAGNY_REPORT` ou NAS) : par défaut U: est testé en premier s'il est accessible.

## Ventilation URSSAF Chalon / Mâcon (département 71)

`prepare_pagny` calcule, comme pour la Drôme / l'Isère, deux **parts d'emploi fret** (NAF 49+52) : numérateur = communes listées (bassins *Chalon* / *Mâcon*), dénominateur = emploi fret de **tout** le département 71. Les communes se modifient en tête de `calculer_ventilation_fos_sete` dans le script.

Pour l'**onglet Bassins**, l'**ID d'agrégat** OPSAM peut être : (1) lu **automatiquement** dans les mêmes classeurs que le cordon (`liste_zones_chalon60min_distincation_paca.xlsx` / `liste_zones_macon60min_distincation_paca.xlsx`, ou repli `liste_zones_chalon_60min.xlsx` / `liste_zones_macon_60min.xlsx`) sous le dossier `PAGNY_REPORT`, feuilles **`zone_chalon`** et **`zone_macon`** (le script prend le **premier entier &gt; 0** de l'onglet) ; (2) donné par `ventilation_zones_chalon_macon.json` s'il vaut remplir ou surcharger ; (3) donné par variables d'environnement. L'analyse **cordon** 1 h utilise en général la **première feuille** des mêmes fichiers (comportement inchangé).

Pour le déploiement en ligne, placer des copies dans le dossier `data/` de l'app et définir les mêmes variables (ou seulement les chemins par défaut relatifs).
