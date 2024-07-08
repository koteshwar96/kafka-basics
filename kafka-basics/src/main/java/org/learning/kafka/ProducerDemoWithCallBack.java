package org.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallBack {

    public static void main(String[] args) {
        // Kafka's "sticky partitioning" mechanism aims to optimize producer performance by
        // sticking to a specific partition for as long as possible when producing messages.
        // This helps achieve better batching and increased throughput.
        log.info("Hello I'm kafka Producer Demo that explains about sticky partitioning");

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

        // some more useful properties to control the batching
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "500");
        //waits for batch size of 500 bytes before it writes to a partition

        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        // waits for 10 ms before it sends data to broker.
        // producer sends data when either of the criteria meets LINGER_MS_CONFIG/ BATCH_SIZE_CONFIG

        // Create ProducerDemo
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // Usually if we publish data, it distributed in a round-robin fashion across partitions
        // but now, as they are published immediately, all those are batched together and published
        // in one go to a single partition. This is called sticky partitioning
        for (int i = 0; i < 10; i++) {

            //Producer record to published to the topic
            ProducerRecord producerRecord = new ProducerRecord<>("demo-first-topic", "Hello Kafka!! " + i);

            //send data. This is async nature and producer cleverly pushes data to partitions
            // using BATCH_SIZE_CONFIG/LINGER_MS_CONFIG so that we have better throughput
            producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                if (Objects.isNull(exception)) {
                    log.info("Data successfully published to topic {}, partition: {}, offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.info("Exception occurred while publishing data to the partition. Exception: {}",exception.getMessage());
                }
            });
        }

        //tell producer to send all the data and block until its done. Sync call
        producer.flush();

        // closing the producer
        producer.close();



    }
}

//producer acks property and min.insync properties (can be configured both at the topic and the broker-level)
// lets us control the after how many writes producer considers that as successful and max fail-overs we can have
// https://www.conduktor.io/kafka/kafka-topic-configuration-min-insync-replicas/.
//  ProducerConfig.ACKS_CONFIG

// Kafka reties infinitely (greater than 2.0)  if not acked as configured using above properties but with a hard timeout of below one
//  ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG

// ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
// This setting determines the maximum number of unacknowledged requests that can be sent
// to the broker at any given time on a single connection (similar to window size in TCP )

// ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
// Ensures that messages are not duplicated in the event of retries by producer
// using a combination of producer ID and sequence numbers assigned to each message
// If this is not enabled and maxInFlight is more than 1, then we can have out of order issue

// safe properties for a producer (these are default for kafka > 3.0 )
// acks=all;
// min.insync.replicas=2 at topic or broker level
// ENABLE_IDEMPOTENCE_CONFIG=true
// reties = max_int (retry until delivery timeout)
// delivery.timeout = 120000ms
// MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5 to have max performance while keeping the ordering

// Compression
// Compression can be done at producer lever or at broker level
// by default producer doesnt enable compression. Choose compression that has better cpu/compression ratio
// compression properties at broker level decides on messages are stored at broker i.e. as it is / decompress / compress to other format
// compression.type = producer / none / <compressionType>

// producer config for high throughput. We need to play with these properties to improve out throughput
// linger.ms = 20
// ProducerConfig.COMPRESSION_TYPE_CONFIG = snappy
// batch.size = 32Kb