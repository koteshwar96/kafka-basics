package org.learning.kafka.opensearch;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {

        String indexName = "wikimedia";

        // creation of openSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // creation of kafka clients
        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();
        kafkaConsumer.subscribe(List.of("wikimedia.recentchange"));


        try (openSearchClient; kafkaConsumer) {

            // creation of open search index
            boolean indexPresent = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (!indexPresent) {
                CreateIndexResponse createIndexResponse = openSearchClient.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
                log.info("Index successfully created. Response: {}", createIndexResponse);
            } else {
                log.info("index {} is already present", indexName);
            }

            // consuming records from kafka topic and publishing to open search
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(300));

                for (ConsumerRecord<String, String> record : consumerRecords) {

                    try {
                        // using id to achieve idempotence for atLeastOnce behaviour
                        String id = getIdForIdempotence(record);

                        IndexRequest indexRequest = new IndexRequest(indexName)
                                .source (record.value(), XContentType.JSON)
                                .id(id);
                        // when we try to add a record/data with same id, then openSearch updates it instead of adding new entry

                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info("Data added and response is: {}", indexResponse.getId());
                    } catch (Exception e) {
                        log.error("Error while publishing data to open search. Error: {}", e.getMessage());
                    }

                    // If we want more control on commits, we can disable auto commit and commit after process each batch
                    // i.e here by calling kafkaConsumer.commitSync();

                }
            }
        }
    }

    private static String getIdForIdempotence(ConsumerRecord<String, String> record) {

        // strategy 1
        // using kafka co-ordinates of the message as Id
        // return record.topic() + "_" + record.partition()+ "_" + record.offset();

        // strategy 2
        // using id present in the record
        return JsonParser.parseString(record.value())
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();

    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

}