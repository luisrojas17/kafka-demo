package com.acme.messaging.kafka.core.consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerString {

    public static final String GROUP_ID = "java_application_consumer";

    private Properties properties;

    private final KafkaConsumer<String, String> consumer;

    public ConsumerString(final String bootstrapServerHost, List<String> topicsToSubscribe) {

        this.init(bootstrapServerHost);

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(topicsToSubscribe);

        setShutDown();
    }

    public void init(final String bootstrapServerHost) {

        // Create a consumer properties
        this.properties = new Properties();

        // To connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerHost);

        // To connect to cluster
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        //properties.setProperty("security.protocol", "SASL_SSL");
        //properties.setProperty("sasl.jaas.config", "");
        //properties.setProperty("sasl.mechanism", "PLAIN");

        // To serialize and des serialize data sent
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty("group.id", GROUP_ID);

        // none = if we do not have any exist consumer group then fail.
        // That is to say, it will be necessary to start the consumer before to star the application.
        // earliest = To read from the beginning of the topic. That is to say, it will always be read all the messages.
        // when the consumer application will be started.
        // latest = To read from the end of the topic. That is to say, it will always be read the last message.
        properties.setProperty("auto.offset.reset", "earliest");

        // The property partition.assignment.strategy defines that way which are added/remmoved
        // new consumers to consumer group
        // By default org.apache.kafka.clients.consumer.RangeAssignor
        // org.apache.kafka.clients.consumer.CooperativeStickyAssignor
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        // To define static assignment strategy
        // If you assign a group.id it will allow to consumers to track and resume from correct offsets for message processing
        //properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "...");

    }

    public void consume() {

        try {

            // To consume messages through infinite loop
            while (true) {

                //log.info("Waiting for messages...");

                // Time to wait for new messages
                // Offsets are commited when you call .poll() and auto.commit.interval.ms has elapsed
                // See enable.auto.commit = true
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Message received\n[key: {}, value: {}, partition: {}, offset: {}]",
                            record.key(), record.value(), record.partition(), record.offset());
                }

            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown...");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            // To cose the consumer. This will avoid also commit offsets
            consumer.close();

            log.info("Consumer is now gracefully shutdown.");
        }

    }

    /**
     * To ensure data integrity, prevent message loss or duplication, maintain system reliability,
     * and enable faster rebalances within the consumer group. A forceful shutdown can interrupt
     * a consumer mid-processing. Graceful shutdown ensures that the consumer finishes processing
     * its current batch of messages and commits their offsets. This prevents messages from being
     * lost or re-processed when the consumer restarts or another consumer takes over the partition,
     * which is vital for maintaining data consistency and achieving at-least-once
     * (or exactly-once in some cases) delivery semantics.
     */
    public void setShutDown() {

        // To get the main thread
        final Thread mainThread = Thread.currentThread();

        final Runnable onShutdown = () -> {
            log.info("Detecting shutdown signal...");
            consumer.wakeup();

            // To join the main thread to allow the execution of the code in the main thread
            try {

                mainThread.join();

            } catch (InterruptedException e) {
                log.error("Unexpected exception", e);
            }

        };

        // To add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(onShutdown));
    }

    public static void main(String[] args) {
        ConsumerString consumer =
                new ConsumerString("localhost:9092", List.of("third_topic"));
        consumer.consume();
    }
}
