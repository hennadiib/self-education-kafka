package kafka.totorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeysDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerKeysDemo.class.getName());
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
            String topic = "first_topic";
            String value = "Hey from Producer with key demo " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            logger.info("Key: " + key);

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
            }).get(); // block send to make it synchronise
        }
        producer.flush();
        producer.close();
    }
}
