# 🎬 Movie Ticket Real-Time Data Engineering Pipeline

An end-to-end Data Engineering project that simulates a movie ticket booking platform using **TMDB API, Python, Apache Kafka, PySpark Structured Streaming, MySQL, Apache Airflow, Docker, and Power BI**.

The project demonstrates movie-data ingestion, real-time booking-event streaming, data transformation, data quality validation, workflow orchestration, and analytics visualization.

---

## 🏗️ Architecture

```text
                    ┌─────────────────┐
                    │    TMDB API     │
                    │ Movie Metadata  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Python      │
                    │   ETL Process   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     MySQL       │
                    │  Movies Table   │
                    └─────────────────┘


┌──────────────────────┐
│ Python Booking       │
│ Event Producer       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│    Apache Kafka      │
│   booking_events     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ PySpark Structured   │
│ Streaming            │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│        MySQL         │
│ Booking Analytics    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Power BI        │
│ Analytics Dashboard  │
└──────────────────────┘

             ▲
             │
       Apache Airflow
             │
             ▼
     Movie Data Pipeline
       ┌──────────────┐
       │ Load Movies  │
       │      ↓       │
       │ Validate     │
       └──────────────┘


🚀 Project Overview

This project simulates the data engineering backend of a movie ticket booking application.

Movie Data Pipeline

Movie metadata is retrieved from the TMDB API using Python and loaded into MySQL.

The movie data is transformed with additional analytical fields:

Rating category
Popularity category
Movie age

Apache Airflow orchestrates the movie ingestion and data-quality validation workflow.

Real-Time Booking Pipeline

A Python producer continuously generates simulated booking events and publishes them to an Apache Kafka topic.

PySpark Structured Streaming consumes the Kafka events and performs:

JSON parsing
Schema validation
Data validation
Timestamp transformation
Booking-hour extraction
Revenue categorization

The processed booking data is stored in MySQL for analytics.

Analytics

Power BI connects to MySQL and provides an interactive dashboard for analyzing booking volume, ticket demand, movie performance, city revenue, and hourly booking patterns.

🛠️ Technologies Used
Technology	Purpose
Python	API ingestion and booking-event generation
TMDB API	Movie metadata source
Apache Kafka	Real-time event streaming
PySpark Structured Streaming	Real-time data processing
MySQL	Data storage and analytics
Apache Airflow	Workflow orchestration and validation
Docker	Infrastructure containerization
Power BI	Analytics and visualization
Git/GitHub	Version control and project hosting
📂 Project Structure
movie-ticket-real-time-data-engineering/
│
├── airflow/
│   ├── dags/
│   │   └── movie_ticket_dag.py
│   └── docker-compose.yml
│
├── docs/
├── sql/
│
├── booking_producer.py
├── booking_stream.py
├── load_movie.py
├── validate_movies.py
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
└── Movie_Ticket_Analytics.pbix
🔄 End-to-End Data Flow
TMDB API
   ↓
Python ETL
   ↓
MySQL Movies
   ↓
Airflow Validation


Booking Producer
   ↓
Apache Kafka
   ↓
PySpark Structured Streaming
   ↓
Validation & Transformation
   ↓
MySQL Booking Analytics
   ↓
Power BI Dashboard
🎥 Movie Data Pipeline

The load_movie.py script retrieves movie metadata from TMDB and loads it into the MySQL movies table.

Transformations include:

rating_category
popularity_category
movie_age

Existing movie records are updated using MySQL upsert logic.

⚡ Real-Time Booking Pipeline

The booking_producer.py script generates simulated booking events and publishes them to Kafka.

Example event:

{
  "booking_id": "B1001",
  "movie_id": 12345,
  "movie_name": "Example Movie",
  "city": "Chennai",
  "theater": "PVR Cinemas",
  "tickets": 3,
  "ticket_price": 200,
  "total_amount": 600,
  "booking_status": "CONFIRMED",
  "booking_time": "2026-09-08T20:15:32.123456"
}

Kafka topic:

booking_events
🔥 PySpark Streaming

booking_stream.py uses PySpark Structured Streaming to consume booking events from Kafka.

Processing Steps
Kafka JSON Event
      ↓
Parse JSON
      ↓
Apply Schema
      ↓
Validate Records
      ↓
Create Booking Date
      ↓
Extract Booking Hour
      ↓
Calculate Revenue Category
      ↓
Write to MySQL
Revenue Categories
< ₹500        → Low
₹500–₹999     → Medium
≥ ₹1000       → High
Data Validation

The streaming pipeline validates:

Booking ID
Movie ID
Ticket count
Ticket price
Total amount
Booking status
Booking timestamp

Only valid booking events are written to the analytics table.

🤖 Apache Airflow

Airflow orchestrates the movie data pipeline.

DAG
load_movies
     ↓
validate_movies
Load Movies

Retrieves and transforms movie metadata before loading it into MySQL.

Validate Movies

Performs data-quality checks including:

Required-field validation
Duplicate movie ID detection
Rating validation
Popularity validation
Empty-table validation

The validation task fails when data-quality rules are not satisfied.

🗄️ MySQL

MySQL is used as the storage and analytics layer.

Movies Table

Stores movie metadata and derived attributes.

Important fields:

movie_id
title
release_date
rating
popularity
language
overview
rating_category
popularity_category
movie_age
Booking Analytics Table

Stores processed real-time booking events.

Important fields:

booking_id
movie_id
movie_name
city
theater
tickets
ticket_price
total_amount
booking_status
booking_time
booking_date
booking_hour
revenue_category
📊 Power BI Dashboard

The Power BI dashboard provides an interactive view of the booking analytics.

KPIs
Total Bookings
Confirmed Bookings
Confirmed Tickets
Confirmed Revenue
Visualizations
Daily Booking Trend
Revenue by City
Revenue by Movie
Bookings by Hour

The dashboard helps analyze booking patterns, movie performance, city-wise revenue, and customer demand by hour.

🐳 Docker

Docker is used to run the Kafka and Airflow infrastructure.

Kafka:

localhost:9092

Airflow:

http://localhost:8080
▶️ How to Run
1. Clone the repository
git clone https://github.com/Suryaselvakumar0702/movie-ticket-real-time-data-engineering.git

cd movie-ticket-real-time-data-engineering
2. Create Python virtual environment
python -m venv venv

Windows:

.\venv\Scripts\Activate.ps1
3. Install dependencies
pip install -r requirements.txt
4. Configure environment variables

Create a .env file based on .env.example.

Add your own credentials:

TMDB_API_KEY=your_tmdb_api_key

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_mysql_username
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=movie_ticket_db

Never commit .env to GitHub.

5. Start Kafka
docker compose up -d

Create the Kafka topic:

docker exec -it movie-kafka /opt/kafka/bin/kafka-topics.sh --create --topic booking_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
6. Load movie data
python load_movie.py
7. Validate movie data
python validate_movies.py
8. Start booking producer

Open another terminal:

python booking_producer.py
9. Start PySpark streaming

Open another terminal:

python booking_stream.py
10. Start Airflow
cd airflow
docker compose up -d

Open:

http://localhost:8080

Run the:

movie_ticket_pipeline

DAG.

11. Open Power BI

Open:

Movie_Ticket_Analytics.pbix

Refresh the data to view the latest analytics.

🔐 Security

Sensitive credentials are stored in environment variables.

The following files are excluded from Git:

.env
venv/
checkpoints/
__pycache__/
*.pyc

Only .env.example is included as a configuration template.

⚠️ Data Source Note

Movie metadata is obtained from the TMDB API.

The booking events in this project are synthetically generated by the Python Kafka producer to simulate real-time movie-ticket booking activity.

They are not real transactions from BookMyShow, TicketNew, or another ticketing company.

This allows the project to demonstrate a real-time streaming architecture without using private transaction data.

🎯 Skills Demonstrated
Python
SQL
ETL
REST API integration
Apache Kafka
PySpark Structured Streaming
Real-time data processing
Data validation
MySQL
Apache Airflow
Docker
Power BI
Git/GitHub
Data pipeline orchestration
💼 Resume Project Description

Movie Ticket Real-Time Data Engineering Pipeline

Built an end-to-end data engineering pipeline using Python, TMDB API, Kafka, PySpark Structured Streaming, MySQL, Airflow, Docker, and Power BI. Implemented real-time booking-event ingestion through Kafka, PySpark transformations and validation, MySQL analytics storage, Airflow-based movie data orchestration, and an interactive Power BI dashboard for booking and revenue analysis.

👨‍💻 Author

Surya Selvakumar

Data Engineering | Python | SQL | PySpark | Kafka | Airflow | MySQL | Power BI

⭐ If you find this project useful, feel free to explore the repository.


### After pasting

In VS Code:

**Ctrl + S**

Then open your VS Code terminal and run:

```powershell
git status