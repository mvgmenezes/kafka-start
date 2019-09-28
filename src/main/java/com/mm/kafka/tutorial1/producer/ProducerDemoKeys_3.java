package com.mm.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys_3 {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys_3.class);

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


        //produzindo varias mensagens
        for (int i=0; i<10;i++){

            //WORKING WITH KEYS
            String topic = "first_topic";
            String value = "Hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info(key);

            //3 - create the producer records
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

            //4 - send data - async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error(e.getLocalizedMessage());
                    }
                }
            });
        }



        //how this send() is async is necessary use the flush(),
        producer.flush();

        //or close() - close is flush() and end together
        producer.close();





    }
}
