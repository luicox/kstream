package com.example.kafka;

import com.luico.kstream.KStream0PropertiesFactory_1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Kafka0TransactionProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "standin_topico_entrada";
    private static final int ACCOUNT_COUNT = 100;
    private static final int MESSAGES_PER_SECOND = 10;

    private final Map<String, AtomicInteger> accountAmounts = new HashMap<>();
    private final List<String> accountKeys = new ArrayList<>();
    private final AtomicInteger accountIndex = new AtomicInteger(0);

    private final Producer<String, String> producer;

    public Kafka0TransactionProducer() {
        Properties props = new KStream0PropertiesFactory_1().build();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);

        for (int i = 1; i <= ACCOUNT_COUNT; i++) {
            String account = "account-" + i;
            accountKeys.add(account);
            accountAmounts.put(account, new AtomicInteger(0));
        }
    }

    public void startSending() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        final long intervalMillis = 1000L / MESSAGES_PER_SECOND;

        scheduler.scheduleAtFixedRate(() -> {
            String account = getNextAccount();
            int amount = accountAmounts.get(account).incrementAndGet();
            String value = String.format("{\"amount\":\"%d.00\"}", amount);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, account, value);
            producer.send(record);

            // Descomenta esto si deseas ver el envío en consola
             System.out.printf("Sent: key=%s value=%s%n", account, value);

        }, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private String getNextAccount() {
        int index = accountIndex.getAndIncrement() % ACCOUNT_COUNT;
        return accountKeys.get(index);
    }

    public static void main(String[] args) {
        Kafka0TransactionProducer producer = new Kafka0TransactionProducer();
        producer.startSending();
        System.out.printf("Producer started, sending %d messages per second in sequence...%n", MESSAGES_PER_SECOND);
    }
}
