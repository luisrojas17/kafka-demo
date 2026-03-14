package com.acme.messaging.kafka.core;

import com.acme.messaging.kafka.core.helpers.StringHelper;
import com.acme.messaging.kafka.core.producers.ProducerString;
import com.acme.messaging.kafka.core.producers.callbacks.CommonCallback;
import org.junit.jupiter.api.Test;

class TestProducer {

    public static final String BOOTSTRAP_SERVER_HOST = "localhost:9092";
    public static final String TOPIC_NAME = "second_topic";

    @Test
    void testProducerWithoutCallbackSuccess() {

        ProducerString<String, String> producer =
                new ProducerString<>(BOOTSTRAP_SERVER_HOST, TOPIC_NAME);

        producer.send(StringHelper.buildUUID());

    }

    @Test
    void testProducerWithCallbackSuccess() {
        ProducerString<String, String> producer =
                new ProducerString<>(BOOTSTRAP_SERVER_HOST, TOPIC_NAME, new CommonCallback());

        producer.send(StringHelper.buildUUID());
    }
}
