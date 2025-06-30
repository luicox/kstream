        package com.luico.kstream;

        import org.apache.kafka.clients.consumer.ConsumerConfig;
        import org.apache.kafka.clients.producer.ProducerConfig;
        import org.apache.kafka.common.serialization.Serdes;
        import org.apache.kafka.streams.*;
        import org.apache.kafka.streams.kstream.*;

        import java.util.*;
        import java.util.concurrent.CountDownLatch;

        public class KStream1GlobalKTable {

            public static void main(String[] args) throws Exception {
                // Configuración del stream processing
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


                // 2. Crear el StreamsBuilder
                StreamsBuilder builder = new StreamsBuilder();

                // 3. Definir GlobalKTable para el flag
                GlobalKTable<String, String> standinFlagTable = builder.globalTable(
                        "standin_topico_zero_tecnico",
                        Consumed.with(Serdes.String(), Serdes.String())
                );

                // 4. Definir el stream de entrada
                KStream<String, String> inputStream = builder.stream(
                        "standin_topico_entrada",
                        Consumed.with(Serdes.String(), Serdes.String())
                );

                // 5. Procesamiento con leftJoin al GlobalKTable
                KStream<String, String> processedStream = inputStream.leftJoin(
                        standinFlagTable,
                        (key, value) -> "standin_flag_key", // Usamos una clave constante para el flag
                        (message, flagStatus) -> {
                            System.out.println("Processing message with flag status: " + flagStatus);

                            if ("ACTIVE".equals(flagStatus)) {
                                return null; // Descarta si standin está activo
                            }
                            return processMessage(message); // Procesa el mensaje
                        }
                );

                // 6. Filtrar nulos y enviar a salida
                processedStream
                        .filter((key, value) -> value != null)
                        .to("standin_topico_destino", Produced.with(Serdes.String(), Serdes.String()));

                // 7. Construir y empezar la topología
                Topology topology = builder.build();
                System.out.println(topology.describe());

                final KafkaStreams streams = new KafkaStreams(topology, props);
                final CountDownLatch latch = new CountDownLatch(1);

                Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                    @Override
                    public void run() {
                        streams.close();
                        latch.countDown();
                    }
                });

                try {
                    streams.start();
                    latch.await();
                } catch (Exception e) {
                    System.err.println("Error starting streams: " + e.getMessage());
                    System.exit(1);
                }
                System.exit(0);
            }

            private static String processMessage(String message) {
                // Lógica de procesamiento
                return message.toUpperCase() + " | processed at " + System.currentTimeMillis();
            }
        }