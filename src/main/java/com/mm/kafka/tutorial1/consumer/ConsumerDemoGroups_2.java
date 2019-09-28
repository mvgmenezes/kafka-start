package com.mm.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups_2 {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups_2.class.getName());

        //1 - Criando a configuração
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-five-application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //o consumer precisa deserializar a mensagen enviada pelo o producers
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //configurando o grupo
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //tem tres possibilidades:
            // 1 - earliest - Ler do inicio do topico
            // 2 - latest - Ler somente as novas mensagens
            // 3 - none - quando nao tem salvo
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );

        //2 - create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //3 - subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic));
        //caso queria assinar mais de um topico, nao devo usar o singleton, uso o array
        //consumer.subscribe(Arrays.asList("first_topic","second_topic","..."));

        //4 - pool for new data
        while(true){
            //100 million seconds
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            //looking into the records
            for(ConsumerRecord<String, String> record: consumerRecords){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }


        }

    }
}
