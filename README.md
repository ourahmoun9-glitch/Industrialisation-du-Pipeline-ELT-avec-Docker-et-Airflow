# 🎬 youtube-elt-pipeline

> End-to-end ELT pipeline extracting YouTube channel data via API, orchestrated with Apache Airflow, stored in a PostgreSQL Data Warehouse — fully containerized with Docker Compose.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.2-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis)

---

## 📋 Table des matières

- [Architecture](#-architecture)
- [Stack technique](#-stack-technique)
- [Structure du projet](#-structure-du-projet)
- [Data Warehouse](#-data-warehouse)
- [Pipeline ELT](#-pipeline-elt)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Utilisation](#-utilisation)
- [Résultats](#-résultats)

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                    │
│                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │ YouTube  │───▶│ Airflow  │───▶│ Staging  │              │
│  │  API v3  │    │  DAGs    │    │ yt_api   │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│                       │                │                     │
│                  ┌────▼────┐     ┌─────▼──────┐            │
│                  │  Redis  │     │  Transform  │            │
│                  │ (Celery)│     │ ISO 8601   │            │
│                  └─────────┘     │ video_type  │            │
│                                  └─────┬───────┘            │
│                                        │                     │
│                                  ┌─────▼──────┐            │
│                                  │    Core     │            │
│                                  │   yt_api    │            │
│                                  └─────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠 Stack technique

| Outil | Version | Rôle |
|---|---|---|
| **Apache Airflow** | 2.9.2 | Orchestration des DAGs |
| **PostgreSQL** | 15 | Data Warehouse (staging + core) |
| **Redis** | 7.2 | Broker Celery |
| **pgAdmin** | 4 | Interface graphique PostgreSQL |
| **Docker Compose** | — | Conteneurisation des 6 services |
| **Python** | 3.12 | Logique ELT et transformations |
| **psycopg2** | — | Driver PostgreSQL |
| **YouTube Data API** | v3 | Source de données |

---

## 📁 Structure du projet

```
youtube-elt-pipeline/
├── .env                           # Variables d'environnement (non versionné)
├── .env.example                   # Template des variables
├── docker-compose.yml             # Orchestration des 6 services Docker
├── requirements.txt               # Dépendances Python
│
├── dags/
│   ├── main.py                    # DAGs Airflow (produce_json + load_to_dwh)
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   └── videos_status.py       # Extraction API YouTube (pagination + batch)
│   │
│   └── datawarehouse/
│       ├── __init__.py
│       ├── dwh.py                 # Tâches Airflow staging_table + core_table
│       ├── data_loading.py        # Chargement fichier JSON
│       ├── data_modification.py   # Insert / Update / Delete (upsert)
│       ├── data_transformation.py # ISO 8601 → TIME, video_type Shorts/Normal
│       └── data_utils.py          # PostgresHook, connexion psycopg2
│
├── sql/
│   └── init_warehouse.sql         # Schémas et tables PostgreSQL
│
└── data/                          # Fichiers JSON générés (gitignore)
    └── YT_data_YYYY-MM-DD.json
```

---

## 🐘 Data Warehouse

Le DWH est structuré en **2 couches** dans la base `elt_db` :

### Couche STAGING — Données brutes

```sql
CREATE TABLE staging.yt_api (
    "Video_ID"       VARCHAR(11) PRIMARY KEY,
    "Video_Title"    TEXT,
    "Upload_Date"    TIMESTAMP,
    "Duration"       VARCHAR(20),   -- Format ISO 8601 brut (ex: PT10M30S)
    "Video_Views"    INT,
    "Likes_Count"    INT,
    "Comments_Count" INT
);
```

### Couche CORE — Données transformées

```sql
CREATE TABLE core.yt_api (
    "Video_ID"       VARCHAR(11) PRIMARY KEY,
    "Video_Title"    TEXT,
    "Upload_Date"    TIMESTAMP,
    "Duration"       TIME,          -- Converti en HH:MM:SS (ex: 00:10:30)
    "Video_Type"     VARCHAR(10),   -- "Shorts" (≤60s) ou "Normal" (>60s)
    "Video_Views"    INT,
    "Likes_Count"    INT,
    "Comments_Count" INT
);
```

---

## 🔄 Pipeline ELT

### DAG 1 — `produce_json` (schedule: `0 14 * * *`)

```
get_playlist_id ──▶ get_video_ids ──▶ extract_video_details ──▶ save_to_json ──▶ trigger_load_to_dwh
```

| Tâche | Description |
|---|---|
| `get_playlist_id` | Récupère l'ID playlist uploads via `forHandle` |
| `get_video_ids` | Pagination complète avec `nextPageToken` (50 vidéos/page) |
| `extract_video_details` | Batch de 50 vidéos · stats + snippet + contentDetails |
| `save_to_json` | Sauvegarde `/opt/airflow/data/YT_data_{date}.json` |
| `trigger_load_to_dwh` | Déclenche automatiquement le DAG 2 via `TriggerDagRunOperator` |

### DAG 2 — `load_to_dwh` (schedule: `None`)

```
staging_table ──▶ core_table
```

| Tâche | Description |
|---|---|
| `staging_table` | Upsert données brutes · suppression data drift |
| `core_table` | Transformation ISO 8601 · video_type · upsert core |

### Transformations ELT

```python
# ISO 8601 → durée lisible
parse_duration("PT1H2M30S")  →  01:02:30  (type TIME PostgreSQL)

# Classification vidéo métier
duration ≤ 60s  →  "Shorts"
duration  > 60s  →  "Normal"

# Upsert — mise à jour métriques dynamiques (views, likes, comments)
INSERT INTO core.yt_api (...)
ON CONFLICT ("Video_ID") DO UPDATE SET
    "Video_Views" = EXCLUDED."Video_Views",
    "Likes_Count" = EXCLUDED."Likes_Count",
    "Comments_Count" = EXCLUDED."Comments_Count"

# Data drift — suppression des vidéos retirées de la chaîne
DELETE FROM staging.yt_api
WHERE "Video_ID" NOT IN {current_api_video_ids}
```

---

## 🚀 Installation

### Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python 3.12+](https://www.python.org/)
- Clé API YouTube Data v3 ([obtenir ici](https://console.cloud.google.com/))

### 1. Cloner le projet

```bash
git clone https://github.com/votre-username/youtube-elt-pipeline.git
cd youtube-elt-pipeline
```

### 2. Configurer les variables d'environnement

```bash
cp .env.example .env
```

Éditer `.env` avec vos valeurs :

```env
# YouTube API
API_KEY=AIzaSy...votre_clé_ici
CHANNEL_HANDLE=@freecodecamp

# Airflow
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow

# PostgreSQL
METADATA_DATABASE_USERNAME=airflow
METADATA_DATABASE_PASSWORD=airflow
```

### 3. Lancer les services

```bash
# Étape 1 — Initialiser Airflow + PostgreSQL
docker-compose up airflow-init
# Attendre "User airflow created with role Admin" puis Ctrl+C

# Étape 2 — Lancer tous les services
docker-compose up -d

# Étape 3 — Vérifier les 6 conteneurs
docker ps
```

### 4. Initialiser le Data Warehouse

```bash
# Créer les schémas et tables dans elt_db
docker exec postgres psql -U airflow -d elt_db -c "
  CREATE SCHEMA IF NOT EXISTS staging;
  CREATE SCHEMA IF NOT EXISTS core;
"
# Puis exécuter sql/init_warehouse.sql via pgAdmin (http://localhost:5050)
```

---

## ⚙️ Configuration

### Connexion Airflow → PostgreSQL

Dans **Airflow UI → Admin → Connections → +** :

| Champ | Valeur |
|---|---|
| Conn Id | `postgres_db_yt_elt` |
| Conn Type | `Postgres` |
| Host | `postgres` |
| Database | `elt_db` |
| Login | `airflow` |
| Password | `airflow` |
| Port | `5432` |

Ou via CLI :

```bash
docker exec airflow-webserver airflow connections add postgres_db_yt_elt \
  --conn-type postgres --conn-host postgres \
  --conn-login airflow --conn-password airflow \
  --conn-schema elt_db --conn-port 5432
```

### Accès aux interfaces

| Service | URL | Identifiants |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | airflow / airflow |
| **pgAdmin** | http://localhost:5050 | admin@admin.com / admin |

---

## 📊 Utilisation

### Lancer le pipeline

```bash
# Déclencher manuellement DAG 1 (DAG 2 se lance automatiquement après)
docker exec airflow-webserver airflow dags trigger produce_json
```

### Vérifier les données

```bash
# Données brutes en staging
docker exec postgres psql -U airflow -d elt_db \
  -c "SELECT COUNT(*) FROM staging.yt_api;"

# Données transformées en core
docker exec postgres psql -U airflow -d elt_db \
  -c 'SELECT "Video_ID", "Video_Title", "Duration", "Video_Type", "Video_Views"
      FROM core.yt_api LIMIT 5;'
```

---

## 📈 Résultats

Après exécution du pipeline sur la chaîne `@freecodecamp` :

```
staging.yt_api  →  1 000+ vidéos  (données brutes API)
core.yt_api     →  1 000+ vidéos  (Duration en TIME + Video_Type calculé)
```

Exemple de données `core.yt_api` :

| Video_ID | Video_Title | Duration | Video_Type | Video_Views |
|---|---|---|---|---|
| `abc123` | Python Full Course | `04:26:52` | Normal | 12 500 000 |
| `xyz789` | #Shorts Python tip | `00:00:58` | Shorts | 850 000 |

---

## 👥 Auteurs

Projet réalisé dans le cadre de la formation **Data Engineering — Simplon 2026**

---

## 📄 Licence

MIT License — voir [LICENSE](LICENSE)
