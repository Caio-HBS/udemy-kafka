package br.com.caiohbs.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Producer");

        // Create Producer roperties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Message " + i;

                // Create Producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // Send data
                producer.send(record, (metadata, e) -> {

                    // Executes everytime a record is sent or an Exception is thrown
                    if (e == null) {
                        // The record was successfully sent
                        log.info(
                                "Key: {} | Partition: {}", key, metadata.partition()
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
