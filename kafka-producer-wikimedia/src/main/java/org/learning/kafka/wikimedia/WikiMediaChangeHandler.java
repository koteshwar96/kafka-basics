package org.learning.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikiMediaChangeHandler implements EventHandler {

    private KafkaProducer producer;
    private String topic;

    public WikiMediaChangeHandler(KafkaProducer producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("WikiMedia SSE onOpen method invoked");
    }

    @Override
    public void onClosed() {
        log.info("WikiMedia SSE onClose method invoked");
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info("WikiMedia SSE onMessage method invoked");
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {
        log.info("WikiMedia SSE onComment method invoked");

    }

    @Override
    public void onError(Throwable t) {
        log.info("WikiMedia SSE onError method invoked");
    }
}
