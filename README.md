<div align="center">

# 🌦️ Météo Tunisie — National Weather Analytics Platform

**A cloud-native, real-time weather data platform covering 221 cities across all 24 governorates of Tunisia.**

[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-Aiven_SSL-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supabase-4169E1?logo=postgresql&logoColor=white)](https://supabase.com)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)

[Live Dashboard →](https://mohamed-chaari.github.io/projetfedere/) · [API Docs →](https://tunisia-meteo-api.onrender.com/docs)

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Pipeline](#-data-pipeline)
- [API Reference](#-api-reference)
- [Frontend Dashboard](#-frontend-dashboard)
- [Database Schema](#-database-schema)
- [Getting Started](#-getting-started)
- [Configuration](#-configuration)
- [Deployment](#-deployment)
- [Contributing](#-contributing)

---

## 🔭 Overview

**Météo Tunisie** is an end-to-end weather data engineering project that collects, streams, stores, analyses, and visualises meteorological data for the entire Tunisian territory.

| Metric | Value |
|---|---|
| Cities covered | **221** |
| Governorates | **24** |
| Historical records | **~403 000 rows** (2020–2024) |
| Current refresh rate | **every 15 minutes** |
| Forecast horizon | **7 days** |
| Forecast refresh rate | **every 6 hours** |

**Key capabilities:**

- 📡 **Real-time ingestion** — Live weather conditions streamed via Apache Kafka (Aiven Cloud)
- 🗃️ **Historical archive** — Five years of daily data from the Open-Meteo Archive API
- 📊 **Automated analytics** — Daily Airflow pipelines compute monthly trends, temperature peaks, inter-variable correlations, and annual statistics
- 🚨 **Alert system** — Threshold-based weather alerts for extreme conditions
- 🌐 **REST API** — JWT-secured FastAPI backend deployed on Render.com
- 🖥️ **Interactive dashboard** — Vanilla JS frontend with Leaflet maps and Plotly charts, hosted on GitHub Pages

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│   Open-Meteo Archive API          Open-Meteo Live API               │
│   (historical 2020-2024)          (current & 7-day forecast)        │
└──────────────────────┬──────────────────────────┬───────────────────┘
                       │                          │
                       ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      KAFKA PRODUCER  (src/producer.py)              │
│  Topics: weather-historical │ weather-current │ weather-forecast     │
│          weather-alerts     │ weather-dlq (Dead Letter Queue)        │
└──────────────────────────────────────┬──────────────────────────────┘
                                       │  Aiven Kafka (SSL/TLS)
                                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      KAFKA CONSUMER  (src/consumer.py)              │
│  Modes: continuous (production) │ batch (Airflow) │ topic (debug)   │
└──────────────────────────────────────┬──────────────────────────────┘
                                       │  psycopg2 batch upserts
                                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│               SUPABASE POSTGRESQL  (TimescaleDB)                    │
│  weather_historical │ weather_current │ weather_forecast             │
│  weather_alerts │ monthly_averages │ temperature_peaks               │
│  correlations │ annual_stats                                         │
└──────────────┬──────────────────────────────┬────────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────┐    ┌─────────────────────────────────────┐
│  APACHE AIRFLOW (DAGs)   │    │  FASTAPI BACKEND  (api/)            │
│  Daily @ 06:00 UTC       │    │  Deployed on Render.com             │
│  - check_freshness       │    │  /api/dashboard  /api/forecast      │
│  - quality_check         │    │  /api/analysis   /api/alerts        │
│  - monthly_averages      │    │  JWT authentication                 │
│  - peak_detection        │    └────────────────────┬────────────────┘
│  - correlation_matrix    │                         │ HTTP / REST
│  - annual_stats          │                         ▼
└──────────────────────────┘    ┌─────────────────────────────────────┐
                                │  FRONTEND  (frontend/)              │
                                │  Vanilla JS + Leaflet + Plotly      │
                                │  Hosted on GitHub Pages             │
                                └─────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Data Sources** | [Open-Meteo](https://open-meteo.com) · [OpenWeatherMap](https://openweathermap.org) | Historical archive & live weather |
| **Message Broker** | [Apache Kafka](https://kafka.apache.org) on [Aiven Cloud](https://aiven.io) (SSL) | Real-time data streaming |
| **Stream Processing** | Python (`kafka-python`) | Producer & consumer services |
| **Database** | [Supabase](https://supabase.com) PostgreSQL + [TimescaleDB](https://www.timescale.com) | Time-series data storage |
| **Orchestration** | [Apache Airflow](https://airflow.apache.org) (Astronomer runtime 3.2) | Scheduled analytics pipelines |
| **Backend API** | [FastAPI](https://fastapi.tiangolo.com) + Uvicorn | REST API with JWT auth |
| **API Hosting** | [Render.com](https://render.com) | Cloud deployment |
| **Frontend** | Vanilla JS · HTML5 · CSS3 | Dashboard UI |
| **Maps** | [Leaflet.js](https://leafletjs.com) | Interactive geographic maps |
| **Charts** | [Plotly.js](https://plotly.com/javascript/) | Responsive data visualisations |
| **Frontend Hosting** | [GitHub Pages](https://pages.github.com) | Static site deployment |
| **Containerisation** | [Docker](https://docker.com) | Airflow environment |

---

## 📁 Project Structure

```
projetfedere/
├── api/                        # FastAPI backend
│   ├── main.py                 # App entry point, CORS, router registration
│   ├── database.py             # SQLAlchemy connection & query execution
│   ├── dependencies.py         # JWT authentication dependency
│   ├── models/
│   │   └── responses.py        # Pydantic response models
│   └── routers/
│       ├── auth.py             # JWT token endpoints
│       ├── dashboard.py        # Current weather & national summary
│       ├── forecast.py         # 7-day forecast endpoints
│       ├── analysis.py         # Monthly, peaks, correlation, annual
│       └── alerts.py           # Active alerts & alert history
│
├── src/                        # Data pipeline core
│   ├── producer.py             # Kafka producer (historical / current / forecast)
│   ├── consumer.py             # Kafka consumer (upserts to PostgreSQL)
│   ├── analysis/               # Standalone analysis modules
│   └── utils/                  # Cities, Kafka config, validators, weather codes
│
├── dags/                       # Apache Airflow DAGs
│   ├── master_pipeline_dag.py  # Master orchestrator (daily @ 06:00 UTC)
│   ├── monthly_analysis_dag.py
│   ├── peak_detection_dag.py
│   ├── correlation_dag.py
│   ├── annual_stats_dag.py
│   ├── data_quality_dag.py
│   └── shared_logic/           # Reusable analysis functions for DAGs
│
├── frontend/                   # Static dashboard
│   ├── index.html              # Landing / login page
│   ├── dashboard.html          # Real-time national overview
│   ├── realtime.html           # Live city-level conditions
│   ├── forecast.html           # 7-day forecast viewer
│   ├── analysis.html           # Monthly trends
│   ├── peaks.html              # Temperature anomaly explorer
│   ├── correlation.html        # Inter-variable correlation matrix
│   ├── annual.html             # Annual statistics
│   ├── app.js                  # Shared API client & auth logic
│   └── style.css               # Global styles
│
├── db/
│   └── schema.sql              # Full PostgreSQL / TimescaleDB schema
│
├── scripts/
│   ├── run_producer.sh         # Convenience wrapper for the producer
│   └── run_consumer.sh         # Convenience wrapper for the consumer
│
├── tests/                      # Test suite
├── docs/                       # Additional documentation
├── Dockerfile                  # Astronomer Airflow runtime image
├── render.yaml                 # Render.com deployment manifest
├── requirements.txt            # Pipeline dependencies
├── requirements-api.txt        # API-only dependencies
└── .env.example                # Environment variable template
```

---

## 🔄 Data Pipeline

### Producer Modes

Run via `python -m src.producer --mode <MODE>` or `scripts/run_producer.sh <MODE>`.

| Mode | Description |
|---|---|
| `historical` | One-time backfill: daily data 2020–2024 from Open-Meteo Archive for all 221 cities |
| `current` | Infinite loop: live conditions refreshed every **15 minutes** |
| `forecast` | Single run: 7-day daily forecast for all cities (Airflow-friendly) |
| `once` | Single current-weather cycle, then exit (for testing) |
| `capitals` | Current mode limited to 24 governorate capitals (fast demo) |
| `all` | Historical backfill (background thread) + current loop |

### Consumer Modes

Run via `python -m src.consumer --mode <MODE>` or `scripts/run_consumer.sh <MODE>`.

| Mode | Description |
|---|---|
| `continuous` | Three daemon threads consuming all topics in production |
| `batch` | Drain all topics then exit — used by Airflow tasks |
| `topic` | Single topic via `--topic` flag for debugging |

### Kafka Topics

| Topic | Content | PostgreSQL Table |
|---|---|---|
| `weather-historical` | Daily archive records | `weather_historical` |
| `weather-current` | Latest conditions per city | `weather_current` |
| `weather-forecast` | 7-day daily predictions | `weather_forecast` |
| `weather-alerts` | Threshold-triggered alerts | `weather_alerts` |
| `weather-dlq` | Failed / invalid messages | — (Dead Letter Queue) |

### Airflow DAG — `master_weather_pipeline`

Scheduled daily at **06:00 UTC**. Retry policy: 2 retries with 5-minute delay.

```
check_freshness
      │
      ▼
quick_quality_check
      │
      ├──────────────────┐
      ▼                  ▼
monthly_averages    peak_detection
      │                  │
      └────────┬─────────┘
               ▼
       correlation_matrix
               │
               ▼
          annual_stats
               │
               ▼
       pipeline_complete
```

---

## 🔌 API Reference

The API is secured with **JWT Bearer tokens**. Obtain a token from the `/token` endpoint.

**Base URL:** `https://tunisia-meteo-api.onrender.com`

### Authentication

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/token` | Obtain a JWT access token |

### Dashboard

| Method | Endpoint | Description | Auth |
|---|---|---|---|
| `GET` | `/api/dashboard/summary` | National summary (avg temp, humidity, wind…) | ✅ |
| `GET` | `/api/dashboard/current` | Current weather for all cities (filter by `governorate` or `region`) | ✅ |
| `GET` | `/api/dashboard/current/{city}` | Current weather for a specific city | ✅ |

### Forecast

| Method | Endpoint | Description | Auth |
|---|---|---|---|
| `GET` | `/api/forecast/today/national` | Today's forecast for all cities | ✅ |
| `GET` | `/api/forecast/governorate/{governorate}` | 7-day forecast for a governorate | ✅ |
| `GET` | `/api/forecast/{city}` | 7-day forecast for a specific city | ✅ |

### Analysis

| Method | Endpoint | Key Query Params | Auth |
|---|---|---|---|
| `GET` | `/api/analysis/monthly` | `year`, `governorate`, `region` | ✅ |
| `GET` | `/api/analysis/peaks` | `severity`, `city`, `limit` | ✅ |
| `GET` | `/api/analysis/correlation` | `season` (required) | ✅ |
| `GET` | `/api/analysis/annual` | `city`, `year` | ✅ |

### Alerts

| Method | Endpoint | Key Query Params | Auth |
|---|---|---|---|
| `GET` | `/api/alerts/active` | — | Public |
| `GET` | `/api/alerts/history` | `days`, `alert_type`, `region` | ✅ |
| `GET` | `/api/alerts/stats` | — | ✅ |

### Health Check

```
GET /health
```
```json
{
  "status": "ok",
  "service": "Tunisia Meteo API",
  "database": "connected"
}
```

Full interactive documentation is available at [`/docs`](https://tunisia-meteo-api.onrender.com/docs) (Swagger UI) and [`/redoc`](https://tunisia-meteo-api.onrender.com/redoc).

---

## 🖥️ Frontend Dashboard

The frontend is a multi-page application built with **Vanilla JS / HTML5 / CSS3**, requiring no build step. It uses:

- **[Leaflet.js](https://leafletjs.com)** — Interactive map with per-city weather markers
- **[Plotly.js](https://plotly.com/javascript/)** — Time-series charts, heatmaps, and correlation matrices

| Page | File | Description |
|---|---|---|
| Login | `index.html` | JWT authentication |
| Dashboard | `dashboard.html` | Real-time national overview |
| Real-time | `realtime.html` | Live conditions per city |
| Forecast | `forecast.html` | 7-day forecast viewer |
| Analysis | `analysis.html` | Monthly temperature trends |
| Peaks | `peaks.html` | Temperature anomaly explorer |
| Correlation | `correlation.html` | Inter-variable correlation matrix |
| Annual | `annual.html` | Annual statistics & trends |

---

## 🗄️ Database Schema

The database runs on **Supabase PostgreSQL** with **TimescaleDB** for time-series optimisation.

| Table | Rows (approx.) | Description |
|---|---|---|
| `weather_historical` | ~403 000 | Daily archive: 221 cities × 5 years |
| `weather_current` | 221 | Latest reading per city (always overwritten) |
| `weather_forecast` | 1 547 | 7-day forecast per city (221 × 7) |
| `weather_alerts` | Varies | Threshold-triggered alert events |
| `monthly_averages` | Computed | Airflow monthly aggregates |
| `temperature_peaks` | Computed | Statistical anomaly records |
| `correlations` | Computed | Pearson correlations by season |
| `annual_stats` | Computed | Per-city yearly summary |
| `pipeline_runs` | Log | DAG execution audit trail |

`weather_historical` is a **TimescaleDB hypertable** partitioned by `date` in 3-month chunks for fast time-range queries.

The full schema is in [`db/schema.sql`](db/schema.sql).

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Access to an [Aiven Kafka](https://aiven.io) cluster with SSL certificates
- A [Supabase](https://supabase.com) project (PostgreSQL)
- An [OpenWeatherMap](https://openweathermap.org/api) API key (optional backup source)

### 1. Clone & install

```bash
git clone https://github.com/Mohamed-Chaari/projetfedere.git
cd projetfedere

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt       # pipeline dependencies
pip install -r requirements-api.txt  # API dependencies
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in your credentials (see Configuration below)
```

### 3. Initialise the database

Apply the schema to your Supabase project:

```bash
psql "$DATABASE_URL" -f db/schema.sql
```

### 4. Run the data pipeline

```bash
# Backfill 5 years of historical data (one-time)
bash scripts/run_producer.sh historical

# Start the consumer (upserts into PostgreSQL)
bash scripts/run_consumer.sh continuous

# Start live current-weather streaming
bash scripts/run_producer.sh current
```

### 5. Run the API

```bash
uvicorn api.main:app --reload
# API available at http://localhost:8000
# Swagger docs at http://localhost:8000/docs
```

### 6. Open the frontend

Open `frontend/index.html` directly in your browser, or serve it with any static file server:

```bash
python -m http.server 3000 --directory frontend
# Dashboard at http://localhost:3000
```

---

## ⚙️ Configuration

Copy `.env.example` to `.env` and fill in all required values:

```ini
# ── Aiven Kafka (SSL) ────────────────────────────────────────────────
KAFKA_BOOTSTRAP=<your-kafka-host>:<port>
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_CA_FILE=ca.pem          # Download from Aiven Console
KAFKA_CERT_FILE=service.cert
KAFKA_KEY_FILE=service.key

# ── Supabase PostgreSQL ──────────────────────────────────────────────
DB_HOST=<your-supabase-pooler-host>
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.<your-project-ref>
DB_PASSWORD=<your-supabase-password>

# ── Weather Data ─────────────────────────────────────────────────────
TIMEZONE=Africa/Tunis
START_DATE=2020-01-01
END_DATE=2024-12-31
OWM_API_KEY=<your-openweathermap-key>   # optional

# ── API (FastAPI) ─────────────────────────────────────────────────────
JWT_SECRET=<strong-random-secret>
ALLOWED_ORIGINS=https://mohamed-chaari.github.io,http://localhost:3000
```

> **Never commit** your `.env` file or SSL certificate files. They are already listed in `.gitignore`.

---

## 🚢 Deployment

### API — Render.com

The API deploys automatically via [`render.yaml`](render.yaml):

```yaml
buildCommand: pip install -r requirements-api.txt
startCommand: uvicorn api.main:app --host 0.0.0.0 --port $PORT
healthCheckPath: /health
```

Set the following environment variables in the Render dashboard: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `JWT_SECRET`.

### Frontend — GitHub Pages

Push changes to `main`. A GitHub Actions workflow automatically deploys the `frontend/` directory to GitHub Pages.

### Airflow — Docker / Astronomer

```bash
# Start the Airflow environment (Astronomer CLI)
astro dev start

# Or with plain Docker Compose
docker compose up
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "feat: add your feature"`
4. Push to your fork: `git push origin feature/your-feature`
5. Open a Pull Request

Please ensure your code follows the existing style and passes all tests (`pytest tests/`).

---

<div align="center">

Built with ❤️ — **ISIMS Sfax · TD5 Project**

</div>
