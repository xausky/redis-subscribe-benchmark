package io.github.xausky.kafka.example;

import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by xausky on 10/17/16.
 */
public class Consumer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("zookeeper.connect", "localhost:2181");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Default");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Collections.singletonList("test"));
        ConsumerRecords<String, String> records = consumer.poll(10000);
        for (ConsumerRecord<String,String> record : records){
            System.out.println(record.key()+":"+record.value());
        }
        consumer.close();
    }
}
