# Real-Time ML Streaming with Apache Kafka

# Overview

This project demonstrates a real-time machine learning streaming pipeline using Apache Kafka, Kafka Streams, Java, Maven, and Weka.

The application reads rows from the Bike Sharing dataset, streams them through Kafka, predicts bike rental counts using a trained machine learning model, and outputs predictions in real time.

---

# System Architecture

DatasetProducer  
↓  
raw-data topic  
↓  
StreamsProcessor (Kafka Streams + ML Model)  
↓  
predictions topic  
↓  
PredictionConsumer

---

# Dataset

Dataset used:

Bike Sharing Dataset (UCI Machine Learning Repository)

Source:

https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset

File used:

data/hour.csv

Features used:

- temp
- hum
- windspeed

Prediction target:

- cnt

---

# Technologies Used

- Java
- Maven
- Apache Kafka
- Kafka Streams API
- Weka
- Jackson JSON

---

# Project Structure

5785-Kafka/

├── data/  
│   └── hour.csv  

├── model/  
│   └── bike-model.model  

├── src/  
│   └── main/java/com/example/  
│       ├── TrainModel.java  
│       ├── DatasetProducer.java  
│       ├── StreamsProcessor.java  
│       └── PredictionConsumer.java  

├── pom.xml  

└── README.md  

---

# STEP 1 — Build Project

Open terminal in project folder:

```bash
cd "..\5785-Kafka"
```

Compile project:

```bash
mvn clean compile
```

If Maven is not added to PATH:

```bash
"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" clean compile
```

Expected output:

```text
BUILD SUCCESS
```

---

# STEP 2 — Train the Machine Learning Model

Run TrainModel.java:

```bash
mvn exec:java "-Dexec.mainClass=com.example.TrainModel"
```

Or:

```bash
"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.TrainModel"
```

Expected output:

```text
Model trained with temp, hum, windspeed -> cnt
```

This creates:

```text
model/bike-model.model
```

The model is trained offline using:

- temp
- hum
- windspeed

to predict:

- cnt

---

# STEP 3 — Start Kafka Server

Go to Kafka folder:

```bash
cd C:\Kafka\kafka_2.13-4.2.0
```

Generate random UUID:

```bash
.\bin\windows\kafka-storage.bat random-uuid
```

Example output:

```text
s5ZjyWbfT46LFWH2oUBY4g
```

Format Kafka storage:

```bash
.\bin\windows\kafka-storage.bat format --standalone -t s5ZjyWbfT46LFWH2oUBY4g -c config\server.properties
```

Start Kafka server:

```bash
.\bin\windows\kafka-server-start.bat config\server.properties
```

Keep this terminal open.

---

# STEP 4 — Create Kafka Topics

Create raw-data topic:

```bash
.\bin\windows\kafka-topics.bat --create --topic raw-data --bootstrap-server localhost:9092
```

Create predictions topic:

```bash
.\bin\windows\kafka-topics.bat --create --topic predictions --bootstrap-server localhost:9092
```

List topics:

```bash
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

Expected output:

```text
raw-data
predictions
```

---

# STEP 5 — Run Streams Processor

Open terminal in project folder:

```bash
cd "..\5785-Kafka"
```

Run:

```bash
mvn exec:java "-Dexec.mainClass=com.example.StreamsProcessor"
```

Or:

```bash
"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.StreamsProcessor"
```

Expected output:

```text
Streams Processor started...
```

The Streams Processor:

- reads data from raw-data topic
- loads the trained ML model
- predicts bike rental counts
- sends predictions to predictions topic

---

# STEP 6 — Run Prediction Consumer

Open another terminal in project folder:

```bash
cd "..\5785-Kafka"
```

Run:

```bash
mvn exec:java "-Dexec.mainClass=com.example.PredictionConsumer"
```

Or:

```bash
"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.PredictionConsumer"
```

Expected output:

```text
Prediction Consumer started...
```

The consumer continuously reads prediction results from the predictions topic.

---

# STEP 7 — Run Dataset Producer

Open another terminal in project folder:

```bash
cd "..\5785-Kafka"
```

Run:

```bash
mvn exec:java "-Dexec.mainClass=com.example.DatasetProducer"
```

Or:

```bash
"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.DatasetProducer"
```

Expected output:

```text
Sent: {"temp":"0.24","hum":"0.81","windspeed":"0.0"}
```

The producer continuously sends dataset rows to Kafka in real time.

---

# Correct Execution Order

1. Kafka Server
2. StreamsProcessor
3. PredictionConsumer
4. DatasetProducer

---

# Example Output

## Producer

```text
Sent: {"temp":"0.24","hum":"0.81","windspeed":"0.0"}
```

## Streams Processor

```text
RAW MESSAGE:
{"temp":"0.24","hum":"0.81","windspeed":"0.0"}

Prediction generated:
{"predicted_count": 52.3}
```

## Prediction Consumer

```text
Prediction received:
{"predicted_count": 52.3}
```

---

# Model Evaluation

This project uses a regression model, so F1-score is not directly applicable.

Regression evaluation metrics:

- Correlation Coefficient
- MAE
- RMSE

F1-score is commonly used for classification tasks, while this project predicts a continuous numeric value.

---

# Video Demo



The demo video shows: https://drive.google.com/file/d/16wqGeg_AmZUSmnNYKDbOQldLUcI-dbvU/view?usp=sharing

1. Kafka server startup
2. Streams processor running
3. Prediction consumer running
4. Dataset producer sending live records
5. Real-time predictions appearing

---

# Conclusion

This project successfully demonstrates a complete real-time machine learning streaming pipeline using Apache Kafka and Kafka Streams.

The system continuously streams dataset records, processes them with a machine learning model, and publishes prediction results in real time.