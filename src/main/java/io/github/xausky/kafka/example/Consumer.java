package io.github.xausky.kafka.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

        KafkaConsumer<Object,Object> consumer = new KafkaConsumer<Object,Object>(props);
        consumer.subscribe(Collections.singletonList("test"));
        ConsumerRecords<Object, Object> records = consumer.poll(10000);
        for (ConsumerRecord<Object,Object> record : records){
            System.out.println(((GenericRecord)record.value()).get("id")+":"+((GenericRecord)record.value()).get("name"));
        }
        consumer.close();
    }
}
