package com.neolcr.producers;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaAvroProducer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", KafkaAvroSerializer.class.getName());
        properties.put("value.serializer", KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");
        Producer<String, User> producer = new KafkaProducer<>(properties);

        User user = User.newBuilder()
                .setId(1)
                .setName("Felipe")
                .build();

        ProducerRecord<String, User> record = new ProducerRecord<>("sec-topic","key1", user);
        producer.send(record, (metadata, exception)-> {
            if (exception == null) {
                System.out.println("Message sent to topic: " + metadata.topic());
            } else {
                exception.printStackTrace();
            }
        });
        producer.close();
    }
}
