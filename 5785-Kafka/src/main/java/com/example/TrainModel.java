package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import weka.classifiers.functions.LinearRegression;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class TrainModel {
    public static void main(String[] args) throws Exception {
        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("temp"));
        attributes.add(new Attribute("hum"));
        attributes.add(new Attribute("windspeed"));
        attributes.add(new Attribute("cnt"));

        Instances data = new Instances("bike", attributes, 0);
        data.setClassIndex(3);

        BufferedReader br = new BufferedReader(new FileReader("data/hour.csv"));
        String header = br.readLine();
        String[] cols = header.split(",");

        String line;
        while ((line = br.readLine()) != null) {
            String[] v = line.split(",");

            double temp = Double.parseDouble(v[9]);
            double hum = Double.parseDouble(v[11]);
            double windspeed = Double.parseDouble(v[12]);
            double cnt = Double.parseDouble(v[15]);

            DenseInstance instance = new DenseInstance(4);
            instance.setValue(attributes.get(0), temp);
            instance.setValue(attributes.get(1), hum);
            instance.setValue(attributes.get(2), windspeed);
            instance.setValue(attributes.get(3), cnt);

            data.add(instance);
        }

        LinearRegression model = new LinearRegression();
        model.buildClassifier(data);

        weka.core.SerializationHelper.write("model/bike-model.model", model);

        System.out.println("Model trained with temp, hum, windspeed -> cnt");
    }
}