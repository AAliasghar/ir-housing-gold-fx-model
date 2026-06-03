# Interest Rates, Housing, Gold & FX Model

A Python-based quantitative finance project that analyzes and models relationships between interest rates, housing markets, gold prices, and foreign exchange (FX) markets using Apache Airflow for workflow orchestration.

## Overview

This repository implements an automated data pipeline for collecting, processing, and analyzing correlations between four key financial market indicators:

- **Interest Rates (IR)**: Central bank rates and yield curves
- **Housing**: Real estate market prices and indicators
- **Gold**: Precious metals commodity prices
- **Foreign Exchange (FX)**: Currency exchange rates

## Project Structure

```
ir-housing-gold-fx-model/
├── airflow/                 # Apache Airflow DAGs and configurations
│   └── dags/               # Workflow definitions
├── src/                     # Source code for models and analysis
├── config/                  # Configuration files
├── docker-compose.yaml      # Docker Compose setup for local development
├── Dockerfile              # Docker image definition
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Features

- **Automated Data Collection**: Uses Airflow to schedule and orchestrate data extraction from multiple sources
- **Multi-Source Integration**: 
  - Financial data via `yfinance`
  - Iranian market data via `bonbast` (foreign exchange rates)
  - Real estate/market data via `cloudscraper`
  - Persian calendar support via `jdatetime`
- **Data Persistence**: PostgreSQL database for reliable data storage
- **Containerized Deployment**: Docker and Docker Compose for easy setup and scalability

## Technology Stack

### Data Processing & Analysis
- **pandas**: Data manipulation and analysis
- **SQLAlchemy**: SQL toolkit and ORM
- **psycopg2**: PostgreSQL adapter

### Data Sources & Web Scraping
- **yfinance**: Yahoo Finance data (gold, FX rates)
- **bonbast**: Iranian foreign exchange and market data
- **cloudscraper**: Web scraping with cloud protection bypass
- **lxml**: XML and HTML parsing

### Orchestration & Infrastructure
- **Apache Airflow**: Workflow scheduling and monitoring
- **PostgreSQL**: Data warehouse backend
- **Docker & Docker Compose**: Containerization

### Utilities
- **jdatetime**: Persian/Jalali calendar support

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.10+
- PostgreSQL 13+

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/AAliasghar/ir-housing-gold-fx-model.git
   cd ir-housing-gold-fx-model
   ```

2. **Start the services with Docker Compose**
   ```bash
   docker-compose up -d
   ```

   This will start:
   - **PostgreSQL**: Data warehouse (accessible on port 5433)
   - **Airflow Webserver**: UI for monitoring DAGs (accessible on http://localhost:8081)
   - **Airflow Scheduler**: Background job scheduler

3. **Access Airflow Dashboard**
   - Navigate to: `http://localhost:8081`
   - Default credentials: Check Airflow documentation for setup

4. **Verify Database Connection**
   ```bash
   PGPASSWORD=quant_password psql -h localhost -U quant_user -d ir_market_data -p 5433
   ```

### Local Development (Without Docker)

1. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize Airflow database**
   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow db init
   ```

## Configuration

Key configurations are stored in the `config/` directory. Database connection details for Docker Compose:

- **Host**: `postgres` (within Docker network) or `localhost` (from host)
- **Port**: `5433`
- **Username**: `quant_user`
- **Password**: `quant_password`
- **Database**: `ir_market_data`

Update these credentials in your environment or configuration files as needed for production use.

## Airflow DAGs

Workflow definitions are located in `airflow/dags/`. Each DAG represents a data pipeline:
- Data ingestion from external sources
- Data transformation and cleaning
- Storage in PostgreSQL
- Analysis and correlation computation

## Dependencies

See `requirements.txt` for the complete list of dependencies:
- pandas
- psycopg2-binary
- sqlalchemy<2.0.0
- cloudscraper
- bonbast
- lxml
- yfinance
- jdatetime

## Environment Variables

Key environment variables used by Docker Compose:

```
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://quant_user:quant_password@postgres/ir_market_data
AIRFLOW__WEBSERVER__SECRET_KEY=super-secret-key-123
AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://quant_user:quant_password@postgres:5432/ir_market_data
```

**⚠️ Important**: Change the `AIRFLOW__WEBSERVER__SECRET_KEY` and database passwords for production deployments.

## Common Commands

### Docker Compose
```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f airflow-webserver

# Stop services
docker-compose down

# Clean up volumes (⚠️ deletes data)
docker-compose down -v
```

### Airflow CLI (Inside Docker)
```bash
# List DAGs
docker-compose exec airflow-webserver airflow dags list

# Trigger a DAG
docker-compose exec airflow-webserver airflow dags trigger <dag_id>

# View DAG status
docker-compose exec airflow-webserver airflow dags status <dag_id>
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

## License

This project is currently unlicensed. See LICENSE file for details.

## Author

[AAliasghar](https://github.com/AAliasghar)

## Support

For issues, questions, or suggestions, please open an [Issue](https://github.com/AAliasghar/ir-housing-gold-fx-model/issues) on GitHub.

---

**Last Updated**: June 2026  
**Status**: Active Development
