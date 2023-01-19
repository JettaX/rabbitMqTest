package org.example.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

public class Sender {
    private static final String EXCHANGE_NAME = "DoubleDirect";

    public static void main(String[] argv) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("pass");
        startChannel(factory, "hello java", "java");
        startChannel(factory, "hello c++", "c++");
        startChannel(factory, "hello php", "php");

    }

    private static void startChannel(ConnectionFactory factory, String message, String topic) throws IOException {
        new Thread(() -> {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                sender(channel, message, topic);
            } catch (IOException | TimeoutException  | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void sender(Channel channel, String message, String topic) throws IOException, InterruptedException {
        while (channel.isOpen()) {
            channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
            Thread.sleep(5000);
        }
    }
}
