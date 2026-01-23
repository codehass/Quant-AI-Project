# Quant-AI Project

**Quant-AI** is a robust, data-driven system designed for real-time high-frequency price prediction of the BTC/USDT trading pair. It leverages a distributed infrastructure to ingest raw market data from Binance, process it using Apache Spark and Airflow, and generate actionable insights via Machine Learning models served through a FastAPI backend.

## 🚀 Features

-   **Real-time Prediction**: High-frequency price forecasting for BTC/USDT.
-   **REST API**: fast and asynchronous API built with **FastAPI**.
-   **Data Pipeline**: Automated ETL workflows orchestrated by **Apache Airflow**.
-   **Data Processing**: Scalable data processing with **Apache Spark**.
-   **Containerization**: Fully containerized environment using **Docker** and **Docker Compose**.
-   **Database**: Reliable data storage with **PostgreSQL**.

## 🛠 Tech Stack

-   **Language**: Python 3.10+
-   **Backend**: FastAPI, Pydantic, SQLAlchemy
-   **Orchestration**: Apache Airflow
-   **Data Processing**: Apache Spark, Pandas, NumPy
-   **Machine Learning**: Scikit-learn
-   **Database**: PostgreSQL
-   **Infrastructure**: Docker, Docker Compose

## 📋 Prerequisites

Ensure you have the following installed on your machine:

-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)

## ⚡ Getting Started

Follow these steps to set up the project locally.

### 1. Clone the Repository

```bash
git clone https://github.com/codehass/Quant-AI-Project.git
cd Quant-AI-Project
```

### 2. Configure Environment Variables

Create a `.env` file from the example template. This file contains database credentials and API configurations.

```bash
cp .env.example .env
```

> **Note**: You can leave the default values in `.env` for local development, or update them as needed (e.g., `DATABASE_PASSWORD`, `SECRET_KEY`).

### 3. Build and Run Services

Start the entire application stack using Docker Compose. This command builds the images and starts the containers in detached mode.

```bash
docker-compose up -d --build
```

**Services started:**
-   `postgres`: Database for application and Airflow.
-   `airflow-init`: One-time initialization for Airflow (database & user).
-   `airflow`: Airflow webserver and scheduler.
-   `backend`: FastAPI application.

## 🖥 Usage

### Accessing the API

Once the containers are running, the backend API is accessible at:

-   **API Root**: `http://localhost:8000`
-   **Interactive Docs (Swagger UI)**: `http://localhost:8000/docs`
-   **Alternative Docs (ReDoc)**: `http://localhost:8000/redoc`

### Accessing Airflow

Manage and monitor your data pipelines via the Airflow web interface:

-   **URL**: `http://localhost:8080`
-   **Default Credentials**:
    -   **Username**: `admin`
    -   **Password**: `admin`

*(These credentials are defined in `airflow/init_airflow.sh`)*

### Database Access

The PostgreSQL database is exposed on port `5433` (mapped from container port 5432) to avoid conflicts with local Postgres instances.

-   **Host**: `localhost`
-   **Port**: `5433`
-   **User/Pass**: Defined in your `.env` file (default: `postgres_user_name` / `your_password`)
-   **Database**: `quant_ai_db`

## 📂 Project Structure

```
Quant-AI-Project/
├── airflow/            # Airflow DAGs, config, and init scripts
│   ├── dags/           # ETL and ML pipelines defined as DAGs
│   └── Dockerfile      # Airflow image configuration
├── app/                # FastAPI source code
│   ├── api/            # Route handlers
│   ├── db/             # Database connection and models
│   ├── main.py         # Application entrypoint
│   └── Dockerfile      # Backend image configuration
├── data/               # Local volume for data storage
├── ml/                 # Machine Learning resources
│   ├── notebooks/      # Jupyter notebooks for exploration
│   └── models/         # Trained model artifacts
├── requirements.txt    # Python dependencies
├── docker-compose.yml  # Docker services definition
└── readme.md           # This documentation
```

## 🤝 Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any enhancements or bug fixes.

## 📄 License

This project is licensed under the MIT License.
