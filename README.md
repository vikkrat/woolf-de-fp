# Olympic Data Engineering Project

A comprehensive data engineering project implementing both **streaming** and **batch data pipelines** for Olympic sports analytics using modern tools including Apache Spark, Kafka, MySQL, and Apache Airflow.

## 📋 Project Overview

This project demonstrates end-to-end data engineering capabilities by:

- **Streaming Pipeline**: Real-time data processing using Kafka and Spark Streaming
- **Batch Pipeline**: ETL processing with Bronze-Silver-Gold architecture 
- **Data Orchestration**: Workflow management using Apache Airflow
- **Analytics**: Sports performance analytics and insights

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │ -> │  Streaming Layer │ -> │   Batch Layer   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         v                       v                       v
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│     MySQL       │    │      Kafka       │    │   Data Lake     │
│   (OLTP Data)   │    │   (Streaming)    │    │ Bronze/Silver/  │
└─────────────────┘    └──────────────────┘    │     Gold        │
                                               └─────────────────┘
```

### Data Lake Architecture
- **Landing**: Raw data from external sources
- **Bronze**: Raw data stored in Parquet format with minimal processing
- **Silver**: Cleaned, validated, and deduplicated data
- **Gold**: Business-ready analytics and aggregated data

## 🚀 Quick Start

### 1. Automated Setup
```bash
# Make setup script executable
chmod +x setup_environment.sh

# Run complete environment setup (Ubuntu/Debian)
./setup_environment.sh
```

### 2. Manual Setup (if needed)
See [Manual Installation Guide](#manual-installation) below.

### 3. Start Services
```bash
# Start all required services
./manage_services.sh start

# Check service status
./manage_services.sh status
```

### 4. Test Your Setup
```bash
# Run connectivity tests to verify everything works
python test_connectivity.py

# Check service status
./manage_services.sh status
```

### 5. Access Applications
- **Airflow UI**: http://localhost:8080 (admin/admin123)
- **MySQL**: localhost:3306 (dataeng/password123)
- **Kafka**: localhost:9092

## 📁 Project Structure

```
woolf-de-fp/
├── dags/
│   ├── project_solution.py        # Airflow DAG
│   ├── landing_to_bronze.py       # Bronze layer processing
│   ├── bronze_to_silver.py        # Silver layer processing
│   └── silver_to_gold.py          # Gold layer processing
├── scripts/
│   ├── populate_data.py           # Database setup & sample data
│   ├── streaming_pipeline.py      # Spark streaming application
│   ├── kafka_producer.py          # Kafka data producer
│   └── spark_utils.py             # Spark utility functions
├── data/
│   ├── landing/                   # Raw CSV files
│   ├── bronze/                    # Parquet files (raw)
│   ├── silver/                    # Parquet files (cleaned)
│   ├── gold/                      # Parquet files (analytics)
│   └── mysql-connector-j-8.0.32.jar # MySQL JDBC driver
├── config/
│   └── project_config.py          # Configuration settings
├── logs/                          # Application logs
├── setup_environment.sh           # Automated setup
├── manage_services.sh             # Service management
├── test_connectivity.py           # Connectivity tests
├── requirements.txt               # Python dependencies
├── README.md                      # This file
└── PROJECT_SUMMARY.md             # Implementation summary
```

## 🔄 Running the Pipelines

### Streaming Pipeline
```bash
# Terminal 1: Start Kafka producer
cd /mnt/d/Projects/Data\ Engineering/woolf-de-fp
source ~/data_engineering_env/bin/activate
python scripts/kafka_producer.py

# Terminal 2: Start Spark streaming
python scripts/streaming_pipeline.py
```

### Batch Pipeline
```bash
# Option 1: Via Airflow UI
# 1. Go to http://localhost:8080
# 2. Enable 'olympic_data_pipeline' DAG
# 3. Trigger the DAG

# Option 2: Direct execution
cd /mnt/d/Projects/Data\ Engineering/woolf-de-fp
python dags/landing_to_bronze.py
python dags/bronze_to_silver.py
python dags/silver_to_gold.py
```

## 📊 Data Analytics

The Gold layer contains various analytics tables:

1. **athlete_performance_summary**: Individual athlete statistics
2. **country_analytics**: Country-level medal performance  
3. **sport_analytics**: Sport participation and statistics
4. **medal_trends**: Medal trends over time
5. **physical_attributes_analysis**: Physical characteristics by sport

### Sample Queries
```sql
-- Top performing countries
SELECT country_noc, total_medals, medal_rank 
FROM gold.country_analytics 
ORDER BY total_medals DESC LIMIT 10;

-- Athletes by performance category
SELECT performance_category, COUNT(*) as athlete_count
FROM gold.athlete_performance_summary
GROUP BY performance_category;
```

## 🛠️ Technologies Used

- **Apache Spark 3.4.0**: Distributed data processing
- **Apache Kafka**: Stream processing platform
- **MySQL 8.0**: Relational database
- **Apache Airflow 2.7.0**: Workflow orchestration
- **Python 3.8+**: Primary programming language
- **PySpark**: Python API for Spark
- **Pandas**: Data manipulation
- **Parquet**: Columnar storage format
- **MySQL JDBC Connector**: Database connectivity (jar file included)

### 📦 Important JAR Dependencies

The project includes the **MySQL JDBC Connector JAR** (`mysql-connector-j-8.0.32.jar`) located in the `data/` directory. This JAR file is essential for:
- Spark-to-MySQL connectivity
- Streaming pipeline database operations
- ETL batch processing with MySQL sources/sinks

The setup automatically detects and configures this JAR file for all Spark applications.

## 🔧 Configuration

Key configuration files:
- `config/project_config.py`: Database and service configurations
- `dags/project_solution.py`: Airflow DAG configuration
- `requirements.txt`: Python package dependencies

## 📈 Monitoring & Logging

- **Airflow Logs**: ~/airflow/logs/
- **Application Logs**: ./logs/
- **Service Logs**: ./logs/ (zookeeper.log, kafka.log, etc.)
- **Quality Reports**: ./logs/quality_report_YYYYMMDD.txt

## 🧪 Data Quality

The pipeline includes comprehensive data quality checks:
- **Validation**: Null checks, data type validation
- **Deduplication**: Remove duplicate records
- **Standardization**: Consistent formatting
- **Range Validation**: Logical value ranges
- **Completeness**: Data completeness scores

## 🚨 Troubleshooting

### Common Issues

1. **MySQL Connection Issues**
   ```bash
   sudo systemctl restart mysql
   mysql -u dataeng -p olympic_dataset
   ```

2. **Kafka Issues** 
   ```bash
   # Check if topic exists
   cd /opt/kafka
   bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

3. **Spark Issues**
   ```bash
   # Check Spark installation
   spark-shell --version
   
   # Check logs
   tail -f /opt/spark/logs/*
   ```

4. **Airflow Issues**
   ```bash
   # Restart Airflow
   ./manage_services.sh stop
   ./manage_services.sh start
   ```

### Performance Tuning
- Adjust Spark executor memory in scripts
- Optimize Kafka partition configuration
- Tune MySQL query performance
- Configure Airflow parallelism

## 📋 Manual Installation

<details>
<summary>Click to expand manual installation steps</summary>

### Prerequisites
- Ubuntu 20.04+ or similar Linux distribution
- 8GB+ RAM (16GB recommended)
- 20GB+ free disk space
- Internet connection

### Step-by-Step Installation

1. **Update System**
   ```bash
   sudo apt update && sudo apt upgrade -y
   ```

2. **Install Java 11**
   ```bash
   sudo apt install -y openjdk-11-jdk
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   ```

3. **Install Python & Dependencies**
   ```bash
   sudo apt install -y python3 python3-pip python3-venv
   python3 -m venv ~/data_engineering_env
   source ~/data_engineering_env/bin/activate
   ```

4. **Install Python Packages**
   ```bash
   pip install -r requirements.txt
   ```

5. **Install MySQL**
   ```bash
   sudo apt install -y mysql-server mysql-client
   sudo mysql_secure_installation
   ```

6. **Install Apache Kafka**
   ```bash
   cd /opt
   sudo wget https://downloads.apache.org/kafka/2.13-3.5.0/kafka_2.13-3.5.0.tgz
   sudo tar -xzf kafka_2.13-3.5.0.tgz
   sudo mv kafka_2.13-3.5.0 kafka
   ```

7. **Install Apache Spark**
   ```bash
   cd /opt  
   sudo wget https://downloads.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
   sudo tar -xzf spark-3.4.0-bin-hadoop3.tgz
   sudo mv spark-3.4.0-bin-hadoop3 spark
   ```

8. **Configure Environment Variables**
   ```bash
   echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
   echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
   echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
   source ~/.bashrc
   ```

9. **Setup Airflow**
   ```bash
   airflow db init
   airflow users create \
       --username admin \
       --password admin123 \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

</details>

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙋‍♂️ Support

For questions or issues:
1. Check the [Troubleshooting](#-troubleshooting) section
2. Review application logs in `./logs/`
3. Create an issue in the repository

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [MySQL Documentation](https://dev.mysql.com/doc/)

---

**Author**: Data Engineering Team  
**Last Updated**: September 2025  
**Version**: 1.0.0