package com.dams.messageparsing.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

/**
 * Thin wrapper around the raw Kafka consumer for a batch-poll pattern.
 * Using kafka-clients directly (not @KafkaListener) gives full control
 * over offset commits and the polling loop.
 *
 * <p>Key difference from Python version: Kerberos ticket acquisition is
 * handled automatically by the JVM's JAAS Krb5LoginModule via the keytab
 * — no external kinit call is required.</p>
 */
@Component
public class KafkaMessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.kafka.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${app.kafka.security-protocol:SASL_SSL}")
    private String securityProtocol;

    @Value("${app.kafka.sasl-mechanism:}")
    private String saslMechanism;

    @Value("${app.kafka.sasl-kerberos-service-name:}")
    private String saslKerberosServiceName;

    @Value("${app.kafka.sasl-kerberos-principal:}")
    private String saslKerberosPrincipal;

    @Value("${app.kafka.keytab-file:}")
    private String keytabFile;

    @Value("${app.kafka.krb5-conf:}")
    private String krb5Conf;

    @Value("${app.kafka.ssl-ca-location:}")
    private String sslCaLocation;

    @Value("${app.kafka.session-timeout-ms:30000}")
    private int sessionTimeoutMs;

    @Value("${app.kafka.heartbeat-interval-ms:10000}")
    private int heartbeatIntervalMs;

    @Value("${app.kafka.max-poll-interval-ms:300000}")
    private int maxPollIntervalMs;

    private KafkaConsumer<String, String> consumer;
    private ConsumerRecord<String, String> currentRecord;

    // Buffer records polled in bulk; serve them one at a time to the batch loop.
    private final Queue<ConsumerRecord<String, String>> buffer = new LinkedList<>();

    public void start() {
        if (krb5Conf != null && !krb5Conf.isBlank()) {
            System.setProperty("java.security.krb5.conf", krb5Conf);
            logger.info("Using krb5.conf: {}", krb5Conf);
        }

        consumer = new KafkaConsumer<>(buildProperties());
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("Subscribed to Kafka topic: {}", topic);
    }

    /**
     * Returns the next message value, or null if the poll timed out with no records.
     * Records are fetched in bulk from Kafka and buffered internally so the batch
     * loop processes one message at a time.
     */
    public String poll(Duration timeout) {
        if (!buffer.isEmpty()) {
            return dequeueAndLog();
        }

        ConsumerRecords<String, String> records = consumer.poll(timeout);
        if (records.isEmpty()) {
            return null;
        }

        for (ConsumerRecord<String, String> r : records) {
            buffer.offer(r);
        }

        return dequeueAndLog();
    }

    /** Commits the offset of the most recently returned record. */
    public void commit() {
        if (currentRecord == null) return;

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
            new TopicPartition(currentRecord.topic(), currentRecord.partition()),
            new OffsetAndMetadata(currentRecord.offset() + 1)
        );
        consumer.commitSync(offsets);
    }

    public void stop() {
        if (consumer != null) {
            consumer.close();
            logger.info("Kafka consumer closed");
        }
    }

    // -------------------------------------------------------------------------

    private String dequeueAndLog() {
        currentRecord = buffer.poll();
        if (currentRecord == null) return null;

        logger.info("Message received from topic={} partition={} offset={}",
            currentRecord.topic(), currentRecord.partition(), currentRecord.offset());
        return currentRecord.value();
    }

    private Properties buildProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put("heartbeat.interval.ms", heartbeatIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put("security.protocol", securityProtocol);

        // Add SASL/Kerberos settings only when a mechanism is configured
        if (saslMechanism != null && !saslMechanism.isBlank()) {
            props.put("sasl.mechanism", saslMechanism);

            if (saslKerberosServiceName != null && !saslKerberosServiceName.isBlank()) {
                props.put("sasl.kerberos.service.name", saslKerberosServiceName);
            }

            // Build JAAS config from keytab — no external kinit needed in Java
            if (saslKerberosPrincipal != null && !saslKerberosPrincipal.isBlank()
                    && keytabFile != null && !keytabFile.isBlank()) {
                String jaas = String.format(
                    "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true keyTab=\"%s\" principal=\"%s\" storeKey=true;",
                    keytabFile, saslKerberosPrincipal);
                props.put("sasl.jaas.config", jaas);
                logger.info("Kerberos JAAS config set for principal: {}", saslKerberosPrincipal);
            }
        }

        // Add SSL CA certificate when protocol requires it
        if (securityProtocol.contains("SSL") && sslCaLocation != null && !sslCaLocation.isBlank()) {
            props.put("ssl.truststore.type", "PEM");
            props.put("ssl.truststore.location", sslCaLocation);
        }

        return props;
    }
}
