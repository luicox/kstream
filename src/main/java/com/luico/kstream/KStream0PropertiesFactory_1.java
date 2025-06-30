package com.luico.kstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KStream0PropertiesFactory_1 {
    public static Properties build() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "standin-processor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-921jm.us-east-2.aws.confluent.cloud:9092"); // Reemplaza con tu endpoint

        // Configuración de seguridad para Confluent Cloud
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username='' " +
                        "password='';");

        // Configuración de procesamiento robusto
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), "2147483647"); // Integer.MAX_VALUE
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), "5");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), "true");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        // Configuración de rendimiento (ajustar según necesidades)
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), "16384");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), "5");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");

        // Configuración de heartbeat y sesión (importante para Cloud)
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "45000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "15000");

        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/state-store");


        return props;
    }
}
