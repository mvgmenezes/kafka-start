package com.mm.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo_1 {
    public static void main(String[] args) {

        String bootstrapserver = "0.0.0.0:9092";
        //1 - create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        //key and value helps kafka to know with type of message needs to be serializer or deserializer
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //2 - create the producer
        //<string, string> means that key and value
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //3 - create the producer records
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello world");

        //4 - send data - async
        producer.send(record);

        //how this send() is async is necessary use the flush(),
        producer.flush();

        //or close() - close is flush() and end together
        producer.close();





    }
}
