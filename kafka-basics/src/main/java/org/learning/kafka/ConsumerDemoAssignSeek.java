package org.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        log.info("Hello I'm Kafka Consumer Demo talking about how to read specific messages from specific partitions");
        // In case you are looking to read specific messages from specific partitions, the .seek() and .assign() API may help you.

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
        // We don't subscribe to any topic and we dont have consumer group concept here
        // instead we only Use consumer assign() and seek() APIs

        // Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition("demo-first-topic", 0);
        long offSetToReadFrom = 2L;
        consumer.assign(List.of(partitionToReadFrom));

        // Seek
        consumer.seek(partitionToReadFrom, offSetToReadFrom);

        int noOfMessagesToRead = 5;

        while(noOfMessagesToRead > 0){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(2000));

            for(ConsumerRecord<String, String> record : records){
                log.info("Record is {} and read from partition: {} , Offset: {}", record.value(), record.partition(), record.offset());
                noOfMessagesToRead--;
                if(noOfMessagesToRead == 0){
                    break;
                }
            }
        }

        log.info("Successfully read the required messages form kafka cluster....");
        consumer.close();

    }
}
