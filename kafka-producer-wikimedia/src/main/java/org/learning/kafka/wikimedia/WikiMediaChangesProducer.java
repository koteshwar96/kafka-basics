package org.learning.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.net.ssl.*;
import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WikiMediaChangesProducer {
    public static void main(String[] args) {
        log.info("Hello I'm exploring here about getting from stream and publishing it to kafka topic");

        String topic = "wikimedia.recentchange";
        String wikiMediaChangeStreamUri = "http://stream.wikimedia.org/v2/stream/recentchange";

        Properties properties = new Properties();
        //properties to connect to local kafka cluster
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //Setting up the producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // As we have high incoming data, setting config for high throughput
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // In case throughput is really really high and our broker is unable to handle the load,
        // buffer.memory  -> This is the max buffer that our producer can keep,
        // post this when we call send method, it will no longer be async.
        // max.block.ms -> post buffer full and send method will throw exception after this much time

        // Create ProducerDemo
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Stream connection
        WikiMediaChangeHandler wikiMediaChangeHandler = new WikiMediaChangeHandler(producer, topic);
        EventSource eventSource = new EventSource.Builder(wikiMediaChangeHandler, URI.create(wikiMediaChangeStreamUri))
                .client(getIgnoreCertificateVerificationClient())
                 .build();

        // Attempts to connect to the remote event source if not already connected.
        // This method returns immediately; the connection happens on a worker thread.
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }


    public static OkHttpClient getIgnoreCertificateVerificationClient(){
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };

        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new OkHttpClient.Builder()
                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0])
                .hostnameVerifier(new HostnameVerifier() {
                    @Override
                    public boolean verify(String hostname, SSLSession session) {
                        return true;
                    }
                })
                .build();
    }
}
