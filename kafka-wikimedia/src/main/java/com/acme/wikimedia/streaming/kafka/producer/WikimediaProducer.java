package com.acme.wikimedia.streaming.kafka.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This class consumes the streaming events provided by {@link WikimediaProducer#STREAMING_URL} and produce a new event
 * sent it to new topic which is {@link WikimediaProducer#TOPIC_NAME}.
 */
public class WikimediaProducer {

    // https://esjewett.github.io/wm-eventsource-demo/
    // https://codepen.io/Krinkle/pen/BwEKgW?editors=1010
    public static final String STREAMING_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    public static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    public static final String TOPIC_NAME = "wikimedia.recentchange";

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=0 Producer would not wait for acknowledgment (possible data loss)
        // acks=1 Producer will wait for leader acknowledgment (limited data loss)
        // acks=all Leader + replicas acknowledgment (no data loss). Replicas =  replica factor
        // For the last one value cheek min.insync.replicas property

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        BackgroundEventHandler eventHandler = new WikimediaHandler(producer, TOPIC_NAME);

        HttpConnectStrategy connectStrategy =
                HttpConnectStrategy.http(URI.create(STREAMING_URL));
                        //.header("User-Agent", "my-kafka-producer/1.0 (contact: admin@ourcompany.com)");

        // To add headers
        Headers headers = new Headers.Builder()
                .add("User-Agent", "my-kafka-producer/1.0 (contact: admin@ourcompany.com)")
                .build();

        connectStrategy.headers(headers);

        EventSource.Builder eventSourceBuilder =
                new EventSource.Builder(connectStrategy);

        try (BackgroundEventSource eventSource =
                new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder).build()) {

            //Start the producer in another thread
            eventSource.start();

            TimeUnit.MINUTES.sleep(10);

        }

    }

}
