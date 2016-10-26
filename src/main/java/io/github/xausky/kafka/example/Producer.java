package io.github.xausky.kafka.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);

        String userSchema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}, {\"name\":\"name\", \"type\": \"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", 7);
        avroRecord.put("name","user7");

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("test",null, avroRecord);
        try {
            producer.send(record);
        } catch(SerializationException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}