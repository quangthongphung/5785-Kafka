package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.Properties;

public class StreamsProcessor {

    public static void main(String[] args) throws Exception {

        // Load trained ML model
        Classifier model =
                (Classifier) weka.core.SerializationHelper.read(
                        "model/bike-model.model"
                );

        ObjectMapper mapper = new ObjectMapper();

        // Kafka Streams config
        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "bike-prediction-app-final-1"
        );

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.Serdes$StringSerde"
        );

        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.Serdes$StringSerde"
        );

        props.put(
                "auto.offset.reset",
                "earliest"
        );

        StreamsBuilder builder = new StreamsBuilder();

        // Read raw-data topic
        KStream<String, String> rawStream =
                builder.stream("raw-data");

        // Process stream
        KStream<String, String> predictionStream =
                rawStream.mapValues(value -> {

                    System.out.println("=================================");
                    System.out.println("RAW MESSAGE:");
                    System.out.println(value);

                    try {

                        JsonNode json =
                                mapper.readTree(value);

                        System.out.println(
                                "JSON parsed successfully"
                        );

                        // Read fields
                        double temp =
                                Double.parseDouble(
                                        json.get("temp").asText()
                                );

                        double hum =
                                Double.parseDouble(
                                        json.get("hum").asText()
                                );

                        double windspeed =
                                Double.parseDouble(
                                        json.get("windspeed").asText()
                                );

                        System.out.println(
                                "temp = " + temp
                        );

                        System.out.println(
                                "hum = " + hum
                        );

                        System.out.println(
                                "windspeed = " + windspeed
                        );

                        // Create attributes
                        ArrayList<Attribute> attributes =
                                new ArrayList<>();

                        attributes.add(
                                new Attribute("temp")
                        );

                        attributes.add(
                                new Attribute("hum")
                        );

                        attributes.add(
                                new Attribute("windspeed")
                        );

                        attributes.add(
                                new Attribute("cnt")
                        );

                        // Dataset structure
                        Instances dataset =
                                new Instances(
                                        "bike",
                                        attributes,
                                        0
                                );

                        dataset.setClassIndex(3);

                        // Create instance
                        DenseInstance instance =
                                new DenseInstance(4);

                        instance.setValue(
                                attributes.get(0),
                                temp
                        );

                        instance.setValue(
                                attributes.get(1),
                                hum
                        );

                        instance.setValue(
                                attributes.get(2),
                                windspeed
                        );

                        // Unknown target
                        instance.setMissing(
                                attributes.get(3)
                        );

                        dataset.add(instance);

                        // Predict
                        double prediction =
                                model.classifyInstance(
                                        dataset.instance(0)
                                );

                        String result =
                                "{ \"predicted_count\": "
                                        + prediction
                                        + "}";

                        System.out.println(
                                "Prediction generated:"
                        );

                        System.out.println(result);

                        return result;

                    } catch (Exception e) {

                        System.out.println(
                                "ERROR OCCURRED:"
                        );

                        e.printStackTrace();

                        return "{ \"error\": \"" +
                                e.getMessage() +
                                "\" }";
                    }
                });

        // Send to predictions topic
        predictionStream.to("predictions");

        KafkaStreams streams =
                new KafkaStreams(
                        builder.build(),
                        props
                );

        streams.start();

        System.out.println(
                "Streams Processor started..."
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );

        // Keep alive
        Thread.currentThread().join();
    }
}