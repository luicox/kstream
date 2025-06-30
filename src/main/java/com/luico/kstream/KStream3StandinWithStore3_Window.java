package com.luico.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class KStream3StandinWithStore3_Window {

    private static final Logger logger = Logger.getLogger(KStream3StandinWithStore.class.getName());

    public static void main(String[] args) throws Exception {
        // Configuración del stream processing
        Properties props = new KStream0PropertiesFactory_1().build();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "standin_kstream_app");
        StreamsBuilder builder = new StreamsBuilder();

        // Store tipo cuentaId -> List<String> (transacciones)
        StoreBuilder<KeyValueStore<String, List<String>>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("account-transactions-store"),
                Serdes.String(),
                Serdes.serdeFrom(new ListSerializer(), new ListDeserializer())
        );
        builder.addStateStore(storeBuilder);

        // Stream para cambios del flag
        KStream<String, String> flagStream = builder.stream(
                "standin_topico_zero_tecnico",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Topic de entrada
        KStream<String, String> inputStream = builder.stream(
                "standin_topico_entrada",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Repartir el flagStream a todas las instancias
        KStream<String, String> broadcastedFlagStream = flagStream.transformValues(() -> new ValueTransformer<String, String>() {
            @Override
            public void init(ProcessorContext context) {}

            @Override
            public String transform(String value) {
                return value;
            }

            @Override
            public void close() {}
        });

        // Join con el flag actualizado
        inputStream.join(
                        broadcastedFlagStream,
                        (leftValue, rightValue) -> rightValue + "::" + leftValue,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .transform(() -> new TransactionStorageTransformer("account-transactions-store"), "account-transactions-store")
                .to("standin_topico_destino", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        streams.start();
        latch.await();
    }

    // Transformer para guardar en RocksDB y procesar si el flag está inactivo
    static class TransactionStorageTransformer implements Transformer<String, String, KeyValue<String, String>> {
        private final String storeName;
        private KeyValueStore<String, List<String>> stateStore;
        private boolean lastFlagWasActive = true;

        public TransactionStorageTransformer(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.stateStore = (KeyValueStore<String, List<String>>) context.getStateStore(storeName);
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            if (value == null || !value.contains("::")) return null;

            String[] parts = value.split("::", 2);
            String flag = parts[0];
            String actualMessage = parts[1];

            boolean standinActive = "ACTIVE".equalsIgnoreCase(flag);

            if (!standinActive && lastFlagWasActive) {
                logger.info("Detección de cambio ACTIVE → INACTIVE. Ejecutando procesamiento acumulado.");
                processStoredTransactions();
            }
            lastFlagWasActive = standinActive;

            List<String> currentList = this.stateStore.get(key);
            if (currentList == null) currentList = new ArrayList<>();
            logger.info("CurrentList=" + currentList + ", stateStore=" + stateStore + ", key=" + key + ", value=" + actualMessage);
            currentList.add(actualMessage);
            stateStore.put(key, currentList);
            logger.info("Mensaje almacenado para cuenta: " + key + ". Total transacciones acumuladas: " + currentList.size());

            return null;
        }

        private void processStoredTransactions() {
            logger.info("Standin INACTIVO. Procesando TODAS las transacciones acumuladas:");
            try (KeyValueIterator<String, List<String>> all = stateStore.all()) {
                while (all.hasNext()) {
                    KeyValue<String, List<String>> entry = all.next();
                    logger.info("Cuenta: " + entry.key);
                    for (String tx : entry.value) {
                        logger.info("\tTransacción: " + tx);
                    }
                    stateStore.delete(entry.key);
                    logger.info("-- Se procesaron las transacciones acumuladas de la cuenta: " + entry.key);
                }
            }
        }

        @Override
        public void close() {}
    }

    // Serializer seguro para listas
    public static class ListSerializer implements org.apache.kafka.common.serialization.Serializer<List<String>> {
        @Override
        public byte[] serialize(String topic, List<String> data) {
            if (data == null) return null;
            List<String> safe = new ArrayList<>();
            for (String item : data) {
                if (item != null) safe.add(item);
            }
            return String.join(";;", safe).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class ListDeserializer implements org.apache.kafka.common.serialization.Deserializer<List<String>> {
        @Override
        public List<String> deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) return new ArrayList<>();
            String[] parts = new String(data, StandardCharsets.UTF_8).split(";;");
            return new ArrayList<>(Arrays.asList(parts));
        }
    }
}
