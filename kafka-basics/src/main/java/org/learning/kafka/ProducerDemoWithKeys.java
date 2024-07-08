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
public class ProducerDemoWithKeys {

    public static void main(String[] args) {

        log.info("Hello I'm kafka Producer Demo that explains about role of keys in partition assignment for values/data");

        Properties properties = new Properties();

        //properties to connect to local kafka cluster
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // properties to connect to up-stash kafka cluster
//        properties.put("bootstrap.servers", "https://rich-starfish-14365-eu1-kafka.upstash.io:9092");
//        properties.put("sasl.mechanism", "SCRAM-SHA-256");
//        properties.put("security.protocol", "SASL_SSL");
//        properties.put("ssl.truststore.location", "/Users/IN22914340/personal/repos/kafka-beginner-course/kafka-basics/src/main/resources/upstashca.p12");
//
//        properties.put("ssl.truststore.type", "PKCS12");
//        properties.put("ssl.truststore.password", "changeit");
//        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmljaC1zdGFyZmlzaC0xNDM2NSS7bTf5RwHjsME8CS82oRzEmnzUTLhUPJOJHTk\" password=\"ZTk4MzY2NTItYzIwYy00MjMyLWI0MDMtYTU1ODE5MzI5OTNk\";");

        //Setting up the producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());


        // Create ProducerDemo
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // Usually if we publish data, it distributed in a round-robin fashion across partitions.
        // If we provide keys along with the value then all the values with same key always get published to same partition
        for (int i = 0; i < 30; i++) {
            String key = "key_" + i % 5;
            String value = "Hello Kafka!! " + i;

            ProducerRecord producerRecord = new ProducerRecord<>("demo-first-topic", key, value);

            //send data. This is async nature and producer cleverly pushes data to partitions
            // using BATCH_SIZE_CONFIG/LINGER_MS_CONFIG so that we have better throughput
            producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                if (Objects.isNull(exception)) {
                    log.info("Data successfully published to topic {} into partition: {} for key {} with offset as {}", metadata.topic(), metadata.partition(), key, metadata.offset());
                } else {
                    log.info("Exception occurred while publishing data to the partition. Exception: {}", exception.getMessage());
                }
            });

            // adding some sleep so that all the 30 values are not published in one go as
            // producer smartly batches them to improve throughput
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell producer to send all the data and block until its done. Sync call
        producer.flush();

        // closing the producer
        producer.close();


    }
}

// formula used by kafka producer to determine the partition from the key is:
//  Utils.toPositive(Utils.murmur2(serializedKey)) % numPartitions;
// Now, we can see it is dependent on numPartitions, so when we alter numPartitions,
// then the messages with same key might end up in different partition, so ordering gurantees would be lost
// So, it is advised to create a new topic in this case
// If required we can also override, partitioner.class, but not advised