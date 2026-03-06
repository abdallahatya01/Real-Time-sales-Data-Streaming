# Real-Time Sales Streaming Pipeline  
**End-to-End Data Engineering Project** using Kafka, Spark, Airflow & Cassandra

![Project Architecture]([https://via.placeholder.com/800x400.png?text=Sales+Streaming+Pipeline+Architecture](https://assets.grok.com/users/c86278c1-e610-4192-90ea-eae324e89671/generated/13ab7148-5c0b-4537-a12e-4da8410faf11/image.jpg))  

## 📌 Project Overview

This project implements a **real-time data streaming pipeline** that simulates sales transactions, streams them through Apache Kafka, processes and enriches the data using **Apache Spark Structured Streaming**, and stores the aggregated results in **Apache Cassandra** for fast analytical queries.

The entire pipeline is **orchestrated** using **Apache Airflow** (DAG triggers the producer & Spark job).

### Use Case
Simulating an e-commerce sales stream:
- Orders generated continuously (producer)
- Real-time aggregation per country & product (1-minute windows)
- Ranking of best-selling products per country
- Persisted results → ready for dashboarding / analytics

## 🛠 Tech Stack

| Layer              | Technology                          | Purpose                              |
|--------------------|-------------------------------------|--------------------------------------|
| Orchestration      | Apache Airflow                      | Schedule & monitor pipeline          |
| Data Ingestion     | Python + kafka-python               | Generate & publish fake sales events |
| Streaming Platform | Apache Kafka                        | Reliable, scalable event streaming   |
| Stream Processing  | Apache Spark Structured Streaming   | Real-time transformation & aggregation |
| Storage            | Apache Cassandra                    | Fast write/read for time-series data |
| Containerization   | Docker + docker-compose             | Local reproducible environment       |

## ✨ Features

- Continuous fake sales data generation (order_id, country, product, price, quantity, etc.)
- Real-time parsing & cleaning in Spark
- Windowed aggregations (1-minute tumbling windows)
- Product ranking per country using window functions
- Exactly-once semantics with checkpointing
- Watermarking + deduplication
- Cassandra table with proper partitioning

## Project Structure

```text
project/
├── dags/
│   └── my_dag.py                 # Airflow DAG
├── script/
│   ├── producer.py               # Kafka producer (renamed from kafka_producer.py)
│   ├── spark_consumer.py         # Spark streaming job
│   └── entrypoint.sh             # (optional) helper scripts
├── docker-compose.yml            # All services: zookeeper, kafka, spark, cassandra, airflow
├── requirements.txt
└── README.md
