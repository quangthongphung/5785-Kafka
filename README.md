# Real-Time ML Streaming with Apache Kafka

## Overview

This project demonstrates a real-time machine learning streaming pipeline using:

- Apache Kafka
- Kafka Streams
- Java
- Maven
- Weka Machine Learning

The application reads rows from the Bike Sharing dataset, streams them through Kafka, predicts bike rental counts using a trained machine learning model, and outputs predictions in real time.

---

# System Architecture

```text
DatasetProducer
        ↓
   raw-data topic
        ↓
 StreamsProcessor
(Kafka Streams + ML Model)
        ↓
 predictions topic
        ↓
PredictionConsumer

Dataset

Dataset used:

Bike Sharing Dataset (UCI Machine Learning Repository)

Source:
https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset

File used:

data/hour.csv

Machine Learning Model

Model Type:

Linear Regression

Input Features:

temp
hum
windspeed

Prediction Target:

cnt

The ML model is trained offline using Weka and saved as:

model/bike-model.model
Technologies Used
Java
Maven
Apache Kafka
Kafka Streams API
Weka
Jackson JSON
Project Structure
5785-Kafka/
│
├── data/
│   └── hour.csv
│
├── model/
│   └── bike-model.model
│
├── src/
│   └── main/java/com/example/
│       ├── TrainModel.java
│       ├── DatasetProducer.java
│       ├── StreamsProcessor.java
│       └── PredictionConsumer.java
│
├── pom.xml
└── README.md
Kafka Topics

This project uses two Kafka topics:

raw-data
predictions
Build Instructions
Step 1: Open Project Folder
cd "E:\Project 5590\5785-Kafka\5785-Kafka"
Step 2: Compile Project
mvn clean compile

If Maven is not added to PATH:

"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" clean compile
Train the ML Model

Run:

mvn exec:java "-Dexec.mainClass=com.example.TrainModel"

Or:

"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.TrainModel"

After training, the model file will be created:

model/bike-model.model
Kafka Setup
Start Kafka Server

Go to Kafka folder:

cd C:\Kafka\kafka_2.13-4.2.0

Start Kafka:

.\bin\windows\kafka-server-start.bat config\server.properties

Keep this terminal running.

Create Kafka Topics

Create raw-data topic:

.\bin\windows\kafka-topics.bat --create --topic raw-data --bootstrap-server localhost:9092

Create predictions topic:

.\bin\windows\kafka-topics.bat --create --topic predictions --bootstrap-server localhost:9092

List topics:

.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

Expected output:

raw-data
predictions
Running the Application

Run the components in this exact order.

Terminal 1 — Streams Processor
mvn exec:java "-Dexec.mainClass=com.example.StreamsProcessor"

Or:

"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.StreamsProcessor"

Expected output:

Streams Processor started...
RAW MESSAGE:
Prediction generated:
Terminal 2 — Prediction Consumer
mvn exec:java "-Dexec.mainClass=com.example.PredictionConsumer"

Or:

"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.PredictionConsumer"

Expected output:

Prediction Consumer started...
Prediction received:
Terminal 3 — Dataset Producer
mvn exec:java "-Dexec.mainClass=com.example.DatasetProducer"

Or:

"C:\Maven\apache-maven-3.9.16\bin\mvn.cmd" exec:java "-Dexec.mainClass=com.example.DatasetProducer"

Expected output:

Sent: {"temp":"0.24","hum":"0.81", ...}
Correct Execution Order
1. Kafka Server
2. StreamsProcessor
3. PredictionConsumer
4. DatasetProducer
Example Output
Producer
Sent: {"temp":"0.24","hum":"0.81","windspeed":"0.0"}
Streams Processor
RAW MESSAGE:
{"temp":"0.24","hum":"0.81","windspeed":"0.0"}

Prediction generated:
{"predicted_count": 52.3}
Prediction Consumer
Prediction received:
{"predicted_count": 52.3}
Model Evaluation

This project uses a regression model, so F1-score is not directly applicable.

Regression evaluation metrics:

Correlation Coefficient: add your value
MAE: add your value
RMSE: add your value

F1-score is commonly used for classification tasks, while this project predicts a continuous numeric value.