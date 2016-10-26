package io.github.xausky.kafka.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;

/**
 * Created by xausky on 10/17/16.
 */
public class Producer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", Long.toString(System.currentTimeMillis()),"test");
        try {
            producer.send(record);
        } catch(SerializationException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}