package com.acme.messaging.kafka.core.producers.callbacks;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class CommonCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        if (exception != null) {
            log.error("Error while sending message to topic", exception);

            // To handle the exception and break the flow

            return;
        }

        log.info("Message was received.\nTopic name: [{}]\n, Partition: [{}]\n, Offset: [{}]\n, Timestamp: [{}]",
                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());

    }

}
