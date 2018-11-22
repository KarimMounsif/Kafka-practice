package com.github.kmounsif.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "localhost:9092";
        String groupId = "my-sixth-application";
        String topic ="first_topic";

        //latch to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch,
                bootstrapServers,
                topic,
                groupId
        );

        //Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
        }

        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                                String bootstrapServers,
                                String topic,
                                String groupId){
            this.latch = latch;
            //Create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create a consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (properties);
        }

        @Override
        public void run() {
            //Poll for new data
            try {
                while(true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key: " + record.key()  +", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            //Interrupt consumer.poll
            //Throws the exception WakeUpException
            consumer.wakeup();
        }
    }
}
