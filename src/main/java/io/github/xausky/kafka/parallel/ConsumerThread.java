package io.github.xausky.kafka.parallel;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by xausky on 10/20/16.
 */
public class ConsumerThread extends Thread {
    private Metrics metrics;
    private boolean stop = false;
    private int id;
    private KafkaConsumer<Object, Object> consumer;
    public ConsumerThread(Properties props,int id,Metrics metrics){
        this.id = id;
        this.metrics = metrics;
        consumer = new KafkaConsumer<Object, Object>(props);
        consumer.subscribe(Collections.singletonList("test"));
    }
    public void run() {
        Thread.currentThread().setName(String.format("ProducerThread-%03d",id));
        try {
            while (!stop){
                ConsumerRecords<Object, Object> records = consumer.poll(100);
                for (ConsumerRecord<Object, Object> record : records){
                    metrics.consumed();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(consumer!=null){
                consumer.close();
            }
        }
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }
}
