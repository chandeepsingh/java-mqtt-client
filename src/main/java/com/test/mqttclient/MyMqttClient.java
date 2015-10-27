package com.test.mqttclient;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MyMqttClient implements MqttCallback {

    MqttClient myClient;
    MqttConnectOptions connOpt;

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static final String brokerUrl = "tcp://<hostname>:1883";
    static final String mqttClientId = "CS-Java-MQTT-Client";
    static final String myTopic = "<myTopic>";
    static final String outputFileName = "mqtt-output.txt";

    @Override
    public void connectionLost(Throwable t) {
        System.out.println("Connection lost! - " + t.toString());
    }

    @Override
    public void messageArrived(String message, MqttMessage mm) throws Exception {
        if (message.contains("<random string>")) {
            Date messageDate = new Date();
            String currentMessageTimeStamp = dateFormat.format(messageDate);

            String[] messageParts = message.split("/");
            String messageName = messageParts[messageParts.length - 1];
            String messageData = messageName + "|" + new String(mm.getPayload()) + "|" + currentMessageTimeStamp;
            writeOuputToFile(messageData);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken imdt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void runClient() {
        connOpt = new MqttConnectOptions();
        connOpt.setCleanSession(true);

        try {
            myClient = new MqttClient(brokerUrl, mqttClientId);
            myClient.setCallback(this);
            myClient.connect(connOpt);
        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            int subQoS = 0;
            myClient.subscribe(myTopic, subQoS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeOuputToFile(String messageData) throws FileNotFoundException, IOException {
        if (messageData.contains("something")) {
            try (PrintWriter outputFile = new PrintWriter(new BufferedWriter(new FileWriter(outputFileName, true)))) {
                outputFile.println(messageData);
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

        MyMqttClient smc = new MyMqttClient();
        smc.runClient();
    }
}
