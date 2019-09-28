package com.mm.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread_3 {
    public static void main(String[] args) {
       new ConsumerDemoWithThread_3().runIt();
    }

    public void runIt(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread_3.class.getName());

        //1 - Criando a configuração
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-six-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);
        //create the consumer runnable
        logger.info("Creating the consumer thread!");
        Runnable myConsumerRunnable = new ConsumerRunnable(countDownLatch, bootstrapServers, groupId, topic);
        Thread myThread = new Thread(myConsumerRunnable);
        //start the thread
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupter", e );
        } finally {
            logger.info("Application is closing");
        }
    }




    //Separando a logica em uma nova classe para chamar via thread
    public class ConsumerRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic){
            this.countDownLatch = latch;

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
            this.consumer = new KafkaConsumer<String, String>(properties);

            //3 - subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton(topic));
            //caso queria assinar mais de um topico, nao devo usar o singleton, uso o array
            //consumer.subscribe(Arrays.asList("first_topic","second_topic","..."));
        }

        @Override
        public void run() {

            try{
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
            }catch (WakeupException we){
                logger.info("Received shutdown signal!!!");
            }finally {
                consumer.close();
                //tell our main code we are done with the consumer
                countDownLatch.countDown();
            }

        }

        public void shutdown(){
            //the wakeup method is a special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
