package com.luico.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class KStream3StandinWithStore2 {

    private static final Logger logger = Logger.getLogger(KStream3StandinWithStore2.class.getName());

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

        // GlobalKTable para el flag de standin
        GlobalKTable<String, String> standinFlagTable = builder.globalTable(
                "standin_topico_zero_tecnico",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Topic de entrada
        KStream<String, String> inputStream = builder.stream(
                "standin_topico_entrada",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        inputStream.peek((key, value) -> System.out.println("Mensaje recibido del topico standin_topico_entrada -> key: " + key + ", value: " + value))
                .leftJoin(standinFlagTable,
                        (key, value) -> "standin_flag_key",
                        (message, flag) -> {
                            return flag == null ? "INACTIVE" : flag;
                        })
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
            String[] parts = value.split("::", 2);
            System.out.println("value-"+value);
            String realValue = parts.length > 1 ? parts[0] : value;
            String flag = parts.length > 1 ? parts[1] : "INACTIVE";

            boolean standinActive = !flag.contains("INACTIVE");

            List<String> currentList = this.stateStore.get(key);
            if (currentList == null) currentList = new ArrayList<>();
            System.out.println("CurrentList-" + currentList + ", stateStore-" + stateStore + ", key-" + key + ", value-" + realValue);
            currentList.add(realValue);
            stateStore.put(key, currentList);
            System.out.println("Mensaje almacenado para cuenta: " + key + ". Total transacciones acumuladas: " + currentList.size());

            if (!standinActive) {
                System.out.println("Standin INACTIVO. Procesando transacciones acumuladas para cuenta: " + key);
                for (String tx : currentList) {
                    System.out.println("\tTransacción: " + tx);
                }
                stateStore.delete(key);
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
