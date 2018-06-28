package com.kafka.core;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by Administrator on 2017/9/6.
 */
public class KafkaProducer {


    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.print("Args is wrong.");
            return;
        }
        String content = args[0];
        System.out.print("Input : " + content);
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", ConfigUtil.getValByKey("kafka.broker.list"));


        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);

        //Send one message.
        KeyedMessage<String, String> message =
                new KeyedMessage<>(ConfigUtil.getValByKey("kafka.topic"), content);
        producer.send(message);

        producer.close();
    }
}
