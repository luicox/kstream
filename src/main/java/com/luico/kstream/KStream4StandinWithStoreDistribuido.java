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

public class KStream4StandinWithStoreDistribuido {

    private static final Logger logger = LoggerFactory.getLogger(KStream4StandinWithStoreDistribuido.class);

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

        // GlobalKTable para estado del standin
        GlobalKTable<String, String> standinFlagTable = builder.globalTable(
                "standin_topico_zero_tecnico",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Topic de entrada
        KStream<String, String> inputStream = builder.stream(
                "standin_topico_entrada",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        inputStream.peek((key, value) -> logger.info("Mensaje recibido del topico standin_topico_entrada -> key: {} value: {}", key, value))
                .leftJoin(
                        standinFlagTable,
                        (key, value) -> "standin_flag_key", // clave constante
                        (message, flagValue) -> {
                            if (flagValue == null) flagValue = "INACTIVE";
                            logger.info("[JOIN] Flag actual: {} - Mensaje: {}", flagValue, message);
                            return flagValue + "::" + message;
                        }
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

            boolean standinActive = !flag.contains("INACTIVE");

            if (!standinActive && lastFlagWasActive) {
                logger.info("Detección de cambio ACTIVE → INACTIVE. Ejecutando procesamiento acumulado.");
                processStoredTransactions();
                logger.info("Transacción Nueva Procesada para cuenta {} sin Standin : {}", key,actualMessage);
            }else{
                List<String> currentList = this.stateStore.get(key);
                if (currentList == null) currentList = new ArrayList<>();
                logger.info("[STORE] El Standin esta Activo");
                logger.info("[STORE] Cuenta: {}, Transacciones actuales: {}", key, currentList);
                currentList.add(actualMessage);
                stateStore.put(key, currentList);
                logger.info("[STORE] Cuenta: {}, Transacciones nueva Agregada: {}", key,actualMessage);
            }
            lastFlagWasActive = standinActive;

            /*
            */



            return null;
        }

        private void processStoredTransactions() {
            logger.info("Standin INACTIVO. Procesando TODAS las transacciones acumuladas:");
            try (KeyValueIterator<String, List<String>> all = stateStore.all()) {
                while (all.hasNext()) {
                    KeyValue<String, List<String>> entry = all.next();
                    logger.info("Cuenta: {}", entry.key);
                    for (String tx : entry.value) {
                        logger.info("\tTransacción Procesada: {}", tx);
                    }
                    stateStore.delete(entry.key);
                    logger.info("-- Se procesaron las transacciones acumuladas de la cuenta: {}", entry.key);
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
