# SolarX.ai
SolarX.ai is an intelligent monitoring platform for photovoltaic (PV) systems that combines realistic data simulation, streaming infrastructure, and explainable AI to detect and diagnose anomalies in solar farm operations.


## Project Structure Overview

The SolarX.ai platform is organized into modular components, each responsible for a key part of the data pipeline, analytics, or user interface. Below is a high-level overview of the main directories and their roles:

```
solarxai/
├── config/                # Plant configuration files (e.g., plant_config.json)
├── dashboard/             # Streamlit dashboard for real-time monitoring
│   ├── streamlit_app.py   # Main dashboard app
│   ├── requirements.txt   # Dashboard dependencies
│   └── utils/             # Dashboard utility modules (e.g., db_utils.py)
├── flink/                 # Flink stream processing jobs and configs
│   ├── jobs/              # Python jobs for Bronze/Silver/Gold ingestion
│   └── conf/              # Flink configuration files
├── mlflow/                # MLflow experiments, models, and data science notebooks
│   ├── experiments/       # ML code, data loaders, and experiment tracking
│   └── inference_engine/  # Inference microservice for anomaly/forecasting
├── postgres/              # PostgreSQL initialization scripts and schema
│   ├── init/              # SQL files for Medallion schema and tables
│   └── scripts/           # Seeder scripts for populating dimension tables
├── scripts/               # Utility scripts (e.g., Kafka topic creation)
├── simulator/             # PV data simulator (Dockerized)
│   ├── src/               # Simulator source code (main.py, cli.py, etc.)
│   └── config/            # Simulator-specific configs
├── test/                  # Test suites for simulator and consumers
├── docker-compose.yml     # Orchestrates all services (Kafka, Flink, DB, etc.)
├── start-solarxai.sh      # Shell scripts to start/stop the platform
├── stop-solarxai.sh
└── README.md              # Project documentation
```

### Component Descriptions

- **config/**: Contains plant configuration files (e.g., inverter layout, plant metadata) used by the simulator and database seeder.

- **dashboard/**: Houses the Streamlit app for real-time visualization of plant KPIs, inverter status, and detected anomalies. Includes utility modules for database access.

- **docs/**: Documentation and architecture diagrams for thesis and project reference.

- **flink/**: Contains Flink jobs for ingesting, cleaning, and aggregating streaming data from Kafka into the PostgreSQL Medallion architecture (Bronze/Silver/Gold layers).

- **mlflow/**: MLflow experiment tracking, model training scripts, and inference engine for deploying trained models (anomaly detection, forecasting) as microservices.

- **postgres/**: SQL scripts to initialize the Medallion schema and dimension tables in PostgreSQL. Seeder scripts populate tables from plant configuration.

- **scripts/**: Utility shell scripts for tasks like Kafka topic creation.

- **simulator/**: Python-based PV data simulator, configurable for speed, failure rates, and weather. Produces realistic inverter, meteo, and POI meter messages to Kafka.

- **test/**: Contains test suites for validating simulator and consumer logic.

- **docker-compose.yml**: Defines and orchestrates all platform services (Kafka controllers/brokers, Flink, PostgreSQL, dashboard, simulators, inference engines).

- **start-solarxai.sh / end-solarxai.sh**: Shell scripts to start/stop the full platform for local or demo use.

Refer to each directory's README or code comments for further details on configuration and usage.


## Installation
Install Docker and Docker Compose, and clone the repo:
```bash
git clone <repo-url> && cd solarxai
```
All infrastructure is managed via Docker Compose — no local Python installation required for the core platform.

### Development
Install requirements using prefered python environment:
```bash
pipenv install
```

## Quick Start

Start the full platform (Kafka + Flink + PostgreSQL + Dashboard):
```bash
./start-solarxai.sh default
```

To start a x60 speed data simulator:
```bash
./start-solarxai.sh default
```

Stop the platform:
```bash
./end-solarxai.sh 
```

## Examples

Run the simulator with custom parameters inside Docker:
```bash
# High failure scenario at x60 speed
docker-compose up -d pv-simulator-high-failure-fast
```

Run the simulator with a custom CLI:
```bash
docker-compose run --rm simulator-base python src/cli.py \
    --speed x60 \
    --inverter-failure-prob 15.0 \
    --cloudy-day-prob 30.0 \
    --start 2026-01-01 --end 2026-04-01
```

## License

This project is part of an academic thesis and is not currently published under an open-source license.