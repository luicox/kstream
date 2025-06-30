package com.luico.kstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

        public class KStream5StandinWithStoreDistribuido {

    private static final Logger logger = LoggerFactory.getLogger(KStream5StandinWithStoreDistribuido.class);

    public static void main(String[] args) throws Exception {
        Properties props = new KStream0PropertiesFactory_1().build();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "standin_kstream_app");

        Producer<String, String> producer = new KafkaProducer<>(props);
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, List<String>>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("account-transactions-store"),
                Serdes.String(),
                Serdes.serdeFrom(new ListSerializer(), new ListDeserializer())
        );
        builder.addStateStore(storeBuilder);

        GlobalKTable<String, String> standinFlagTable = builder.globalTable(
                "standin_topico_zero_tecnico",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> inputStream = builder.stream(
                "standin_topico_entrada",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> processedStream = inputStream
                .peek((key, value) -> logger.info("Mensaje recibido del topico standin_topico_entrada -> key: {} value: {}", key, value))
                .leftJoin(
                        standinFlagTable,
                        (key, value) -> "standin_flag_key", // clave constante para hacer join
                        (message, flagValue) -> {
                            if (flagValue == null) flagValue = "INACTIVE";
                            logger.info("[JOIN] Flag actual: {} - Mensaje: {}", flagValue, message);
                            return flagValue + "::" + message;
                        }
                )
                .process(() -> new TransactionProcessor("account-transactions-store"), "account-transactions-store");

        // 🔁 Conectar manualmente el stream procesado al topic destino
        processedStream.to("standin_topico_destino", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        streams.start();
        latch.await();
    }

    static class TransactionProcessor implements Processor<String, String, String, String> {
        private final String storeName;
        private KeyValueStore<String, List<String>> stateStore;
        private ProcessorContext<String, String> context;
        private boolean lastFlagWasActive = true;

        public TransactionProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.stateStore = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<String, String> record) {
            String key = record.key();
            String value = record.value();

            logger.info("[PROCESS] key: {}, value: {}", key, value);

            if (value == null || !value.contains("::")) return;

            String[] parts = value.split("::", 2);
            String flag = parts[0];
            String actualMessage = parts[1];

            boolean standinActive = !flag.contains("INACTIVE");

            if (!standinActive && lastFlagWasActive) {
                logger.info("Cambio de estado ACTIVE → INACTIVE. Procesando transacciones acumuladas.");
                processStoredTransactions();
                context.forward(new Record<>(key, actualMessage, record.timestamp()));
                logger.info("Transacción enviada directo para cuenta {}: {}", key, actualMessage);
            }else if(!standinActive && !lastFlagWasActive) {
                logger.info("Standin INACTIVE.");
                processStoredTransactions();
                context.forward(new Record<>(key, actualMessage, record.timestamp()));
                logger.info("Transacción enviada directo para cuenta {}: {}", key, actualMessage);
            }
            else {
                List<String> currentList = stateStore.get(key);
                if (currentList == null) currentList = new ArrayList<>();
                logger.info("[STORE] Mensajes Guardados Previamente {}: {}", key, currentList);
                currentList.add(actualMessage);
                stateStore.put(key, currentList);
                logger.info("[STORE] Transacción guardada en modo standin para cuenta {}: {}", key, actualMessage);

            }

            lastFlagWasActive = standinActive;
        }

        private void processStoredTransactions() {
            try (KeyValueIterator<String, List<String>> iterator = stateStore.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, List<String>> entry = iterator.next();
                    for (String tx : entry.value) {
                        context.forward(new Record<>(entry.key, tx, System.currentTimeMillis()));
                        logger.info("Transaccion Pendiente Procesada para cuenta {}: {}", entry.key, tx);
                    }
                    stateStore.delete(entry.key);
                }
            }
        }

        @Override
        public void close() {}
    }

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
