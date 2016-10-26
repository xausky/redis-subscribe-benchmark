package io.github.xausky.kafka.parallel;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
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
    private KafkaProducer<Object, Object> producer;
    public ProducerThread(Properties props,int size,int id,Metrics metrics){
        this.size = size;
        this.id = id;
        this.metrics = metrics;
        this.producer = new KafkaProducer<Object, Object>(props);
    }
    public void run() {
        Thread.currentThread().setName(String.format("ProducerThread-%03d",id));

        String userSchema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}, {\"name\":\"name\", \"type\": \"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);

        try {
            while (!stop){

                String value = getRandomString(size);
                avroRecord.put("id",value.hashCode());
                avroRecord.put("name",value);
                ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("test",null,avroRecord);
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
