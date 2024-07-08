package org.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {

    public static void main(String[] args) {
        log.info("Hello I'm Kafka Consumer Demo");

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
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // we have offsets.retention.minutes config at broker level, which decides on how many
        // minutes offsets are valid if consumer hasnt read any data

        // Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("demo-first-topic"));

        while(true){

            log.info("Polling now....");

            // Consumer by default reads from Leader of the partition
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1)); //Max time to wait for records

            for(ConsumerRecord<String, String> record : records){
                log.info("Key: {}, value: {} from partition {} ", record.key(), record.value(), record.partition());
            }
        }


    }
}

// Delivery Semantics/strategies
// AtMostOnce: We commit as soon as we receive message, so we may miss out few messages if consumer crashes in middle of processing a batch
// AtLeastOnce: We commit after we process all the messages in the batch, so we may end up processing few messages again
// if consumer crashes in middle of processing a batch, now after it restarts it will start from last commit.
// So we need to have an idempotent consumer
// exactlyOnce: transactional API easy with streams API

// Commit Strategies
// By default, enable.auto.commit = true and auto.commit.interval.ms = 5000,
// that means offsets are committed by consumer, when we call poll() and (after) auto.commit.interval.ms has elapsed
// So, consumer has to make sure in this case that messages are processed synchronously
// before it calls next poll else we would end up in atMostOnce behaviour

// If we want more control, then we can disable auto.commit and manually commitSync() or commitAsync()

// Advanced -> We need to process the messages exactly once and cant find a way to do idempotency, then
// auto.commit= false and store offsets in an external storage like DB maybe
// Then, we need to handle consumer re-balances and consumers need to use seek()

// Consumer Internal Threads
// Heart Beat thread
// heartbeat.interval.ms -> how often consumer sends a heart beat
// session.timeout.ms -> if not heart beat is received in this time, it considers consumer is dead
// If we want fast re-balancing of consumers, then we need to adjust above two config to lower values
// Consumer poll Thread
// max.poll.interval.ms -> max amount of time bw two polls before declaring consumer to be dead
// Adjust this as per the processing time of ur messages

// fetch.min.bytes , fetch.max.bytes, fetch.max.wait.ms
// This lets us control min/max amount of data we need in each poll and max time we will wait

// By default, consumer reads data from the leader of the partition, now if consumer is in different Data center zone,
// We would be charged additionally and would increase our latency as well.
// To read data the consumer in the same data center or close one,
// use client.rack config in consumer config, and
// replica.selector.class to RackAwareReplicaSelector and
// rack.id config must be set for broker

