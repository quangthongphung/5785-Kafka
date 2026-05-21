package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class DatasetProducer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        props.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        BufferedReader br =
                new BufferedReader(new FileReader("data/hour.csv"));

        String header = br.readLine();

        String[] columns = header.split(",");

        String line;

        while ((line = br.readLine()) != null) {

            String[] values = line.split(",");

            StringBuilder json = new StringBuilder("{");

            for (int i = 0; i < columns.length; i++) {

                json.append("\"")
                        .append(columns[i])
                        .append("\":\"")
                        .append(values[i])
                        .append("\"");

                if (i < columns.length - 1) {
                    json.append(",");
                }
            }

            json.append("}");

            String message = json.toString();

            producer.send(
                    new ProducerRecord<>("raw-data", message)
            );

            System.out.println("Sent: " + message);

            Thread.sleep(1000);
        }

        producer.close();

        System.out.println("Finished sending data.");
    }
}