package io.github.xausky.kafka.parallel;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by xausky on 10/20/16.
 */
public class Parallel {
    private static int TEST_DATA_SIZE = 32;
    public static void main(String[] args) throws IOException, InterruptedException {

        int threadCount;
        try {
            threadCount = Integer.parseInt(System.getProperty("threadCount"));
        }catch (Exception e){
            threadCount = 1;
        }

        String acksConfig = System.getProperty("acksConfig");
        if(acksConfig==null){
            acksConfig = "0";
        }

        String brokerHost = System.getProperty("brokerHost");
        if(brokerHost==null){
            brokerHost = "localhost:9092";
        }

		System.out.printf("threadCount:%d,acksConfig:%s,brokerHost:%s\n",threadCount,acksConfig,brokerHost);

        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG,acksConfig);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Default");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

        props.put("schema.registry.url", "http://localhost:8081");
        props.put("zookeeper.connect", "localhost:2181");

        Metrics metrics = new Metrics();

        List<ProducerThread> producers = new ArrayList<ProducerThread>(threadCount);
        List<ConsumerThread> consumers = new ArrayList<ConsumerThread>(threadCount);
        for(int i=0;i<threadCount;i++){
            producers.add(new ProducerThread(props,TEST_DATA_SIZE,i,metrics));
            consumers.add(new ConsumerThread(props,i,metrics));
        }

		Thread.currentThread().sleep(10000);

        for(int i=0;i<threadCount;i++){
            producers.get(i).start();
            consumers.get(i).start();
        }

        Thread.currentThread().sleep(10000);

        for(ProducerThread producer:producers){
            producer.setStop(true);
        }

        Thread.currentThread().sleep(10000);

        for(ConsumerThread consumer:consumers){
            consumer.setStop(true);
        }

        metrics.print();

    }
}
