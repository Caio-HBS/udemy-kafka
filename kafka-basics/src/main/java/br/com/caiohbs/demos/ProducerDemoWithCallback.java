package br.com.caiohbs.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Producer");

        // Create Producer roperties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400");

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {

                // Create Producer record
                ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "Message " + i);

                // Send data
                producer.send(record, (metadata, e) -> {

                    // Executes everytime a record is sent or an Exception is thrown
                    if (e == null) {
                        // The record was successfully sent
                        log.info(
                                "Received new metadata\nTopic:{}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()
                        );
                    } else {
                        log.error("An error occurred while sending the record", e);
                    }

                });

            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        // Flush and close the Producer
        producer.close();

    }

}
