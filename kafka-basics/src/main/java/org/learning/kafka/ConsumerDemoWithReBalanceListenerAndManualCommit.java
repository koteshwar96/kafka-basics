package org.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemoWithReBalanceListenerAndManualCommit {

    public static void main(String[] args) {
        log.info("Hello I'm Kafka Consumer Demo talking about partition re-assignment");
        // Partitions re-assignment usually happens when a new consumer joins or leaves the group, or partitions are being added

        Properties properties = new Properties();

        //properties to connect to local kafka cluster
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // properties to connect to up-stash kafka cluster
//        properties.put("bootstrap.servers", "https://rich-starfish-14365-eu1-kafka.upstash.io:9092");
//        properties.put("sasl.mechanism", "SCRAM-SHA-256");
//        properties.put("security.protocol", "SASL_SSL");
//        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmljaC1zdGFyZmlzaC0xNDM2NSS7bTf5RwHjsME8CS82oRzEmnzUTLhUPJOJHTk\" password=\"ZTk4MzY2NTItYzIwYy00MjMyLWI0MDMtYTU1ODE5MzI5OTNk\";");
//
//        properties.put("ssl.truststore.location", "/Users/IN22914340/personal/repos/kafka-beginner-course/kafka-basics/src/main/resources/upstashca.p12");
//        properties.put("ssl.truststore.type", "PKCS12");
//        properties.put("ssl.truststore.password", "changeit");

//        properties.put("ssl.endpoint.identification.algorithm", "");
//        properties.put("ssl.socket.factory", get());



        //Setting up the producer properties
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // If set, the consumer is treated as a static member, can be used in combination
        // with a larger session timeout to avoid group rebalances caused by transient unavailability
//        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1");

        // This strategy incrementally changes/updates partition assignment to the consumers in the group
        // without stopping all the consumers and while preserving as many existing partition assignments as possible
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName() );
        

        // Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        ConsumerReBalanceListenerImpl consumerReBalanceListener = new ConsumerReBalanceListenerImpl(consumer);

        Thread currentThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            log.info("Invoking Consumer wakeup to gracefully shutdown the consumer from shutdown hook..");
            consumer.wakeup();
            try {
                currentThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));


        try {
            consumer.subscribe(Arrays.asList("demo-first-topic"));

            while(true){

                log.info("Polling now....");
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1)); //Max time to wait for records

                for(ConsumerRecord<String, String> record : records){
                    log.info("Key: {}, value: {} from partition {} ", record.key(), record.value(), record.partition());
                    // updating the local offset map
                    consumerReBalanceListener.updatePartitionOffset(record.topic(), record.partition(), record.offset());
                }
                consumer.commitAsync(); //committing before we poll again
            }

        }catch (WakeupException wakeupException){
            log.info("About to start graceful shutdown");

        } catch (Exception e){
            log.error("UnExpected Error: {}",e.getMessage());
        }finally {
            consumer.commitSync(consumerReBalanceListener.getCurrentOffsets()); // committing before we gracefully shutdown
            consumer.close();
            log.info("Gracefully closed the kafka consumer...");
        }

    }
}
