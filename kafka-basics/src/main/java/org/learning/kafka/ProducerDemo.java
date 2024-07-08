package org.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemo {

    public static void main(String[] args) {
        log.info("Hello I'm a Kafka Producer Demo");

        Properties properties = new Properties();

        //properties to connect to local kafka cluster
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // properties to connect to up-stash kafka cluster
        properties.put("bootstrap.servers", "https://rich-starfish-14365-eu1-kafka.upstash.io:9092");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("ssl.truststore.location", "/Users/IN22914340/personal/repos/kafka-beginner-course/kafka-basics/src/main/resources/upstashca.p12");

        properties.put("ssl.truststore.type", "PKCS12");
        properties.put("ssl.truststore.password", "changeit");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmljaC1zdGFyZmlzaC0xNDM2NSS7bTf5RwHjsME8CS82oRzEmnzUTLhUPJOJHTk\" password=\"ZTk4MzY2NTItYzIwYy00MjMyLWI0MDMtYTU1ODE5MzI5OTNk\";");



        //Setting up the producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // Create ProducerDemo
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //Producer record to published to the topic
        ProducerRecord producerRecord = new ProducerRecord<>("demo-first-topic", "Hello Kafka!!");

        //send data. This is async nature
        producer.send(producerRecord);
        log.info("producer published just now");

        //tell producer to send all the data and block until its done. Sync call
        producer.flush();

        // closing the producer
        producer.close();


    }
}
