package com.luico.kstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KStream2StandinProcessorWithBufferTopicX {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "standin-processor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-56d1g.eastus.azure.confluent.cloud:9092"); // Reemplaza con tu endpoint

// Configuración de seguridad para Confluent Cloud
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username='LBF7MEE2446OSRY5' " +
                        "password='eAYwOebdDzxfcaxbovwjM6jOMAZfMceuSv6zOcb8F3CgkdZ3du/UO8LI9RJFpieJ';");

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
        StreamsBuilder builder = new StreamsBuilder();

        // 1. GlobalKTable para el flag (topic compactado)
        GlobalKTable<String, String> standinFlagTable = builder.globalTable(
                "standin_topico_zero_tecnico",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 2. Stream de entrada principal
        KStream<String, String> inputStream = builder.stream(
                "standin_topico_entrada",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 3. Procesamiento condicional con join
        KStream<String, String>[] branches = inputStream.branch(
                // Predicado para standin activo (mensajes al buffer)
                (key, value) -> {
                    // Implementación conceptual - en la práctica necesitamos join
                    return false; // Se reemplazará con join real
                },
                // Predicado para standin inactivo (mensajes directos)
                (key, value) -> true
        );

        // Versión CORRECTA usando transformador
        KStream<String, String> processedStream = inputStream.transform(
                () -> new StandinAwareTransformer(standinFlagTable),
                Named.as("standin-transformer")
        );

        // Enrutamiento correcto usando filter
        processedStream.filter((key, value) -> value == null)
                .mapValues(v -> "") // Mensajes al buffer
                .to("standin_buffer_topic");

        processedStream.filter((key, value) -> value != null)
                .to("standin_topico_salida");

        // 5. Procesamiento del buffer cuando standin se desactiva
        builder.stream("standin_buffer_topic")
                .leftJoin(
                        standinFlagTable,
                        (key, value) -> "standin_flag_key",
                        (message, flagStatus) -> "INACTIVE".equals(flagStatus) ? processMessage(""+message) : null
                )
                .filter((key, value) -> value != null)
                .to("standin_topico_salida");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            streams.start();
            latch.await();
        } catch (Exception e) {
            System.exit(1);
        }
    }

    static class StandinAwareTransformer implements Transformer<String, String, KeyValue<String, String>> {
        private final GlobalKTable<String, String> standinFlagTable;
        private ProcessorContext context;

        public StandinAwareTransformer(GlobalKTable<String, String> standinFlagTable) {
            this.standinFlagTable = standinFlagTable;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            // Implementación conceptual - en la práctica necesitarías un join
            boolean isActive = false; // Se determinaría con el join real
            if (isActive) {
                return null; // Ir al buffer
            }
            return KeyValue.pair(key, processMessage(value));
        }

        @Override
        public void close() {}
    }

    private static String processMessage(String message) {
        return message.toUpperCase() + " | processed at " + System.currentTimeMillis();
    }
}