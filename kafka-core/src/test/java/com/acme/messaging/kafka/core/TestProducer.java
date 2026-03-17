package com.acme.messaging.kafka.core;

import com.acme.messaging.kafka.core.helpers.StringHelper;
import com.acme.messaging.kafka.core.producers.ProducerString;
import com.acme.messaging.kafka.core.producers.ProducerStringWithKeys;
import com.acme.messaging.kafka.core.producers.callbacks.CommonCallback;
import org.junit.jupiter.api.Test;

class TestProducer {

    public static final String BOOTSTRAP_SERVER_HOST = "localhost:9092";
    public static final String TOPIC_NAME = "second_topic";
    public static final String THIRD_TOPIC_NAME = "third_topic";

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

    @Test
    void testProducerWithKeysSuccess() {
        ProducerStringWithKeys<String, String> producer =
                new ProducerStringWithKeys<>(BOOTSTRAP_SERVER_HOST, THIRD_TOPIC_NAME);

        producer.send(StringHelper.buildUUID());
    }
}
