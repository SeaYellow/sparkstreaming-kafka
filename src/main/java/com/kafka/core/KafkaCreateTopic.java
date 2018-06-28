package com.kafka.core;


import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

import java.util.Properties;

/**
 * Created by Administrator on 2017/9/7.
 */
public class KafkaCreateTopic {

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.print("Args is wrong.");
            return;
        }
        String topic = args[0];
        System.out.print("Input : " + topic);
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", ConfigUtil.getValByKey("kafka.broker.list"));

        ZkClient zkClient = new ZkClient(ConfigUtil.getValByKey("hbase.zookeeper.quorum"), 10000, 10000, new SerializableSerializer());
        if (!AdminUtils.topicExists(zkClient, topic)) {
            AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
            System.out.print("Topic : " + topic + " Create Successful.");
        } else {
            System.out.print("Topic : " + topic + " Has Exists.");
        }
    }
}
