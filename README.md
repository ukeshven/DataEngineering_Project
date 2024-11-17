# Stock Market Real-Time Data Engineering Project
The project contains an end-to-end Data Engineering project that processes real-time stock market data using Apache Kafka, AWS and other technologies. The goal is to simulate, process and analyze stock market data in real time.

# Project Overview

The project simulates real-time stock market data using a CSV dataset and streams the data through Kafka. Data is processed using a Kafka consumer and stored in AWS S3. Further processing and analytics are performed using AWS Glue and Athena.

### Technologies Used
- **Python**: For data simulation, Kafka producer and consumer scripts.
- **Apache Kafka**: For real-time data streaming.
- **AWS S3**: To store processed data.
- **AWS Glue**: To clean and transform data.
- **AWS Athena**: For querying and analyzing data.
- **SQL**: For querying the data in Athena.

## Workflow
1. **Data Simulation**:
   - A CSV dataset is used to simulate real-time stock market data.
   - The Kafka producer sends this data to a Kafka topic.

2. **Real-Time Streaming**:
   - The Kafka consumer reads data from the topic and stores it in AWS S3.

3. **Data Processing**:
   - AWS Glue processes the raw data and prepares it for analysis.

4. **Data Analysis**:
   - AWS Athena queries the processed data to extract insights.
