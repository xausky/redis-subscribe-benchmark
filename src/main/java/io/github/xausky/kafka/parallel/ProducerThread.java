package io.github.xausky.kafka.parallel;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by xausky on 10/20/16.
 */
public class ProducerThread extends Thread {
    private Metrics metrics;
    private int id;
    private int size;
    private boolean stop = false;
    private KafkaProducer<String, String> producer;
    public ProducerThread(Properties props,int size,int id,Metrics metrics){
        this.size = size;
        this.id = id;
        this.metrics = metrics;
        this.producer = new KafkaProducer<String, String>(props);
    }
    public void run() {
        Thread.currentThread().setName(String.format("ProducerThread-%03d",id));
        try {
            while (!stop){
                String value = getRandomString(size);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("test",
                        value
                        ,value);
                producer.send(record);
                metrics.produced();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(producer!=null){
                producer.close();
            }
        }
    }

    public static String getRandomString(int length) {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }
}
