package org.example.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Scanner;

public class Reciever {
    private static final String EXCHANGE_NAME = "DoubleDirect";
    private static final String COMMAND_SET_TOPIC = "set_topic";
    private static final String COMMAND_STOP = "s";
    private static final String COMMAND_HELP = "help";
    private static Connection connection;
    private static HashMap<String, Channel> channels = new HashMap<>();

    public static void main(String[] argv) throws Exception {
        start();
    }

    private static void start() throws Exception {
        if (connection == null) {
            startConnection();
        }
        while (true) {
            System.out.println("Enter command: ");
            String command = new Scanner(System.in).nextLine();
            commandDetector(command);
        }
    }

    private static void commandDetector(String command) {
        String[] commandArray = command.split(" ");
        if (commandArray[0].equals(COMMAND_SET_TOPIC)) {
            try {
                createChannels(commandArray);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (commandArray[0].equals(COMMAND_HELP)) {
            System.out.println("Available commands:");
            System.out.println("set_topic <topic_1> <topic_n> - set topic or topics for listening");
            System.out.println("s - stop listening");
            System.out.println("help - show available commands");
        } else if (commandArray[0].equals(COMMAND_STOP)) {
            stopTopics();
        } else {
            System.out.println("Unknown command. Enter 'help' for help");
        }
    }

    private static void startConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("pass");
        connection = factory.newConnection();
    }

    private static void createChannels(String[] commandArray) throws Exception {
        for (int i = 1; i < commandArray.length; i++) {
            System.out.println("Your topic: " + commandArray[i]);
            createChannel(commandArray[i]);
        }
    }

    private static void createChannel(String topic) throws Exception {
        if (connection == null) {
            startConnection();
        }
        Channel channel = connection.createChannel();
        channels.put(topic, channel);
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, topic);
        System.out.println(" [*] Waiting for messages");
        DeliverCallback deliverCallback = getDeliverCallback();
        consume(queueName, true, deliverCallback, channel, topic);
    }

    private static DeliverCallback getDeliverCallback() {
        return (consumerTag, delivery) -> {
            System.out.println(consumerTag);
            String message = new String(delivery.getBody(), "UTF-8");
            LocalDateTime localDateTime = LocalDateTime.now();
            System.out.println(" [x] Received '" + message + "' at " + localDateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        };
    }

    private static void consume(String queueName, boolean autoAck, DeliverCallback deliverCallback, Channel channel, String topic) throws IOException {
        channel.basicConsume(queueName, autoAck, topic, deliverCallback, consumerTag -> {
        });
    }

    private static void stopConnection() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
            System.out.println("Connection closed");
        } else {
            System.out.println("Connection not found");
        }
    }

    private static void stopTopics() {
        channels.forEach((topic, channel) -> {
            try {
                channel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void stopTopic(String topic) {
        try {
            channels.get(topic).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
