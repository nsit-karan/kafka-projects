package com.github.karana.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {

    //private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // Initialize producer config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Initialize Kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);


        // Send data
        for (int i = 0; i < 5; i++) {
            String topic = "first_topic";
            String key = "id+" + Integer.toString(i);
            String value = "hello world" + Integer.toString(i);
            // Create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("received metadata finally");
                        logger.info("Received new metadata : \n"
                                + "Topic = " + recordMetadata.topic() + "\n"
                                + "Partition = " + recordMetadata.partition() + "\n"
                                + "Offset = " + recordMetadata.offset() + "\n"
                                + "Timestamp = " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
