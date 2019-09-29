package com.mm.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek_4 {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek_4.class.getName());

        //1 - Criando a configuração
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //o consumer precisa deserializar a mensagen enviada pelo o producers
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        //tem tres possibilidades:
            // 1 - earliest - Ler do inicio do topico
            // 2 - latest - Ler somente as novas mensagens
            // 3 - none - quando nao tem salvo
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );

        //2 - create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign and Seek are mostly used to replay data or fetch a specific message

        //Assign first
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartition));

        //Seek
        consumer.seek(topicPartition, offSetToReadFrom);

        int numberOfMessagesToRead = 5; //when find the offset, read more 5 messages
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        //4 - pool for new data
        while(keepOnReading){
            //100 million seconds
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            //looking into the records
            for(ConsumerRecord<String, String> record: consumerRecords){
                numberOfMessagesReadSoFar +=1;

                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; //to exit the while loop
                    break; //to exit the for loop
                }
            }
        }

        logger.info("Exiting the application");
    }
}
