package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final String bootstrapServers = "localhost:9092";
    private static final String topicName = "wikimedia.recentchange";
    private static final String consumerGroupId = "opensearch-consumer-group";
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException, InterruptedException {
        try (RestHighLevelClient openSearchClient = createOpenSearchClient();
             Consumer<String, String> consumer = new KafkaConsumer<>(getProperties())) {
            // check if index exists, create one if not
            String indexName = "wikimedia";
            if (!openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Index 'wikimedia' has been created.");
            } else {
                log.info("Index 'wikimedia' already exists.");
            }

            // subscribe to a topic
            consumer.subscribe(Collections.singleton(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
                log.info(String.format("Consumed %d record(s)", records.count()));
                BulkRequest bulkRequest = new BulkRequest();

                records.forEach(record -> {
                    String id = extractId(record.value());
                    String message = record.value();
                    IndexRequest indexRequest = new IndexRequest(indexName).source(message, XContentType.JSON).id(id);
                    bulkRequest.add(indexRequest);
                });

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info(String.format("Inserted %d records. ", bulkResponse.getItems().length));

                    //Thread.sleep(1000);

                    consumer.commitSync();
                    log.info("Offsets have been committed.");
                }
            }
        }
    }

    private static String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connectionString = "http://localhost:9200";
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connectionString);
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

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }
}
