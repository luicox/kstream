package com.luico.kstream;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Kafka0TopicsManager {

    private final AdminClient adminClient;

    public Kafka0TopicsManager() {
        Properties config =  new KStream0PropertiesFactory_1().build();
        this.adminClient = AdminClient.create(config);
    }

    /**
     * Recreate a list of topics: deletes them if they exist, then creates them again.
     */
    public void recreateTopics(List<String> topicNames, int numPartitions, short replicationFactor) throws Exception {
        Set<String> existingTopics = adminClient.listTopics().names().get();

        // Eliminar los tópicos que ya existen
        List<String> toDelete = new ArrayList<>();
        for (String topic : topicNames) {
            if (existingTopics.contains(topic)) {
                toDelete.add(topic);
            }
        }

        if (!toDelete.isEmpty()) {
            System.out.println("Deleting topics: " + toDelete);
            adminClient.deleteTopics(toDelete).all().get();
            Thread.sleep(2000); // Espera a que se eliminen completamente
        }

        // Crear los nuevos tópicos
        List<NewTopic> newTopics = new ArrayList<>();
        for (String topic : topicNames) {
            newTopics.add(new NewTopic(topic, numPartitions, replicationFactor));
        }

        adminClient.createTopics(newTopics).all().get();
        System.out.println("Created topics: " + topicNames);
    }

    public void close() {
        adminClient.close();
    }

    public static void main(String[] args) {
        List<String> topics = Arrays.asList(
                "standin_topico_entrada",
                "standin_kstream_app-account-transactions-store-changelog",
                "standin_topico_destino"
                //"standin_topico_zero_tecnico"
        );

        int partitions = 6;
        short replication = 3;

        Kafka0TopicsManager manager = new Kafka0TopicsManager();
        try {
            manager.recreateTopics(topics, partitions, replication);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            manager.close();
        }
    }
}
