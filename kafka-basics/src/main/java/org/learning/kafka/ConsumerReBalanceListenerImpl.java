package org.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ConsumerReBalanceListenerImpl implements ConsumerRebalanceListener {

    private KafkaConsumer<String, String> kafkaConsumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public ConsumerReBalanceListenerImpl(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        currentOffsets = new HashMap<>();
    }

    public void updatePartitionOffset(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset+1, null));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("onPartitionsRevoked method is invoked for partitions: {}, committing current offsets", partitions);
        kafkaConsumer.commitSync(currentOffsets); // committing what ever we have processed so far as some partitions are revoked.
        // We can probably flush any local cache associated with this partition or save the data to DB about the progress
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("onPartitionsAssigned method invoked for partitions: {}", partitions);

    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }
}
