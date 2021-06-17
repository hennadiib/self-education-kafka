package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBackDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallBackDemo.class.getName());
        // create producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hey from Producer with call back demo " + i);

            // send data
            producer.send(record, new Callback() {
                @Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        logger.info("Topic: " + recordMetadata.topic());
                        logger.info("Partition: " + recordMetadata.partition());
                        logger.info("Offset: " + recordMetadata.offset());
                        logger.info("Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
