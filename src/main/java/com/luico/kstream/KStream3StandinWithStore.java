package com.luico.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;


public class KStream3StandinWithStore {

    private static final Logger logger = LoggerFactory.getLogger(KStream3StandinWithStore.class);//Logger.getLogger(KStream3StandinWithStore.class.getName());
    private static final AtomicReference<String> standinFlag = new AtomicReference<>("INACTIVE");

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

        // KStream para el flag (control en tiempo real)
        builder.stream("standin_topico_zero_tecnico", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> logger.info("[FLAG UPDATE] key=" + k + ", value=" + v))
                .filter((k, v) -> "standin_flag_key".equals(k))
                .foreach((k, v) -> standinFlag.set(v));

        // Topic de entrada
        KStream<String, String> inputStream = builder.stream(
                "standin_topico_entrada",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        inputStream.peek((key, value) -> logger.info("Mensaje recibido del topico standin_topico_entrada -> key: " + key + ", value: " + value))
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

        public TransactionStorageTransformer(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.stateStore = (KeyValueStore<String, List<String>>) context.getStateStore(storeName);
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            boolean standinActive = !standinFlag.get().contains("INACTIVE");

            List<String> currentList = this.stateStore.get(key);
            if (currentList == null) currentList = new ArrayList<>();
            logger.info("CurrentList=" + currentList + " key=" + key + ", value=" + value);
            currentList.add(value);
            stateStore.put(key, currentList);
            logger.info("Mensaje almacenado para cuenta: " + key + ". Total transacciones acumuladas: " + currentList.size());

            if (!standinActive) {

                logger.info("Standin INACTIVO. Procesando transacciones acumuladas para cuenta: " + key);
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
            return null;
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
