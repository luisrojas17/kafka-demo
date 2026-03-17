package com.acme.wikimedia.streaming.kafka.producer;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;

@Slf4j
public class WikimediaHandler implements BackgroundEventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        log.info("Stream has been opened...");

    }

    @Override
    public void onClosed() throws Exception {

        if (Objects.nonNull(kafkaProducer)) {
            kafkaProducer.close();
        }

        log.info("Producer has been closed.");

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {

        log.info("Sending data [{}]...", messageEvent.getData());

        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        log.error("It was occurred an error to read the event.", t);
    }
}
