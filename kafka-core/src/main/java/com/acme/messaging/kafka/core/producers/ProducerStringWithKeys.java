package com.acme.messaging.kafka.core.producers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerStringWithKeys<K extends String, V extends String> {

    private Properties properties;

    private final String topicName;

    private final KafkaProducer<String, String> producer;

    public ProducerStringWithKeys(final String bootstrapServerHost, final String topicName) {
        this.init(bootstrapServerHost);

        producer = new KafkaProducer<>(properties);

        this.topicName = topicName;
    }

    private void init(final String bootstrapServerHost) {

        // Create a producer properties
        this.properties = new Properties();

        // To connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerHost);

        // To connect to cluster
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        //properties.setProperty("security.protocol", "SASL_SSL");
        //properties.setProperty("sasl.jaas.config", "");
        //properties.setProperty("sasl.mechanism", "PLAIN");

        // To serialize and des serialize data sent
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    }

    public void send(final String message) {

        log.info("Sending message [{}] to topic [{}]", message, topicName);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i <= 10; i++) {

                String key = "id_" + i;

                // To create a producer record
                // The constructor needs a topic_name, message
                // If you do not provide the key, the partition to use can be changed
                // However, If you provide the key,
                // the partition to use will be the same when the message with the key will be the same.
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topicName, key, message);

                // To Send the record
                // This step is made it like asynchronous call
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            log.error("Error while sending message to topic", exception);

                            // To handle the exception and break the flow

                            return;
                        }

                        log.info("Key: [{}] | Partition: [{}].", key, metadata.partition());
                    }
                });

            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // To tell the producer to send all data and block until done -- synchronous
        // See batch.size property
        producer.flush();

        log.info("Message [{}] sent to topic [{}].", message, topicName);

        log.info("Producer was closed.");

        producer.close();

    }

}
