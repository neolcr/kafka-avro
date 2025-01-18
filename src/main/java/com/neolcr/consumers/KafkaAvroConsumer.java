package com.neolcr.consumers;

import com.neolcr.producers.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {
    public static void main(String[]args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", KafkaAvroDeserializer.class.getName());
        properties.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("specific.avro.reader", "true");
        properties.put("group.id", "id");

        Consumer<String, User> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("sec-topic"));

        try{
            while(true){
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, User> record: records){
                    System.out.println("Consumed record with key: " + record.key() + " and value " + record.value());
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
