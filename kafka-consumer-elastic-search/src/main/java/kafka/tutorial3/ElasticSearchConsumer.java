package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient() {
        String hostName = "twitter-kafka-9873896288.eu-central-1.bonsaisearch.net";
        String userName = "";
        String password = "";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                                              .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                                  @Override
                                                  public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                                      return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                                  }
                                              });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elastic-search";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable autocommit of offsets

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        // poll for new data

        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            int recordsCount = consumerRecords.count();
            logger.info("Received: " + recordsCount);

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record: consumerRecords) {
                logger.info("Key: " + record.key());
                logger.info("Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("Offset: " + record.offset());
                //generate ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    // twitter feed specific id
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: " + record.value());
                }
            }
            if (recordsCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offset have been committed");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // client.close();
    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
