package com.jnit.KafkaConsumer.KafkaConsumerFactory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.Duration;
import java.util.*;
public class Receiver {
public void getMessages(String contractID, String fromDate, String toDate) {
    this.dynamicConsumerFactory = (DynamicConsumerFactory) applicationContext.getBean("consumerContainerFactory");
    this.consumerFactory = this.dynamicConsumerFactory.getConsumerFactory();

    // Parse date strings into Instant objects
    var fromInstant = LocalDate.parse(fromDate).atStartOfDay().toInstant(ZoneOffset.UTC);
    var toInstant = LocalDate.parse(toDate).atStartOfDay().toInstant(ZoneOffset.UTC);

    try (var consumer = consumerFactory.createConsumer()) {
        var partitions = consumer.partitionsFor(topic);
        if (partitions != null && !partitions.isEmpty()) {
            Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
            Map<TopicPartition, Long> endOffsets = new HashMap<>();

            partitions.stream()
                .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                .forEach(topicPartition -> {
                    consumer.assign(Collections.singletonList(topicPartition));

                    // Fetch offsets using helper method
                    long beginningOffset = getOffsetForTimestamp(consumer, topicPartition, fromInstant);
                    long endOffset = getOffsetForTimestamp(consumer, topicPartition, toInstant);

                    beginningOffsets.put(topicPartition, beginningOffset);
                    endOffsets.put(topicPartition, endOffset);

                    // Fetch and process messages
                    fetchAndProcessMessages(consumer, contractID, topicPartition, beginningOffset, endOffset);
                });

            Logger.info("Offsets for topic: " + topic);
            beginningOffsets.forEach((partition, startOffset) ->
                Logger.info("Partition " + partition.partition() +
                            " : Beginning offset = " + startOffset +
                            " , End offset = " + endOffsets.get(partition))
            );

        } else {
            Logger.error("No partitions available for topic: " + topic);
        }

    } catch (Exception e) {
        Logger.error("Error occurred while processing Kafka messages", e);
    }
}

private long getOffsetForTimestamp(Consumer<String, String> consumer, TopicPartition topicPartition, Instant timestamp) {
    var timestampsToSearch = Collections.singletonMap(topicPartition, timestamp.toEpochMilli());
    var offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
    return Optional.ofNullable(offsetsForTimes.get(topicPartition))
                   .map(OffsetAndTimestamp::offset)
                   .orElse(consumer.position(topicPartition));
}

private void fetchAndProcessMessages(Consumer<String, String> consumer, String contractID, TopicPartition topicPartition, long beginningOffset, long endOffset) {
    consumer.seek(topicPartition, beginningOffset);

    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        boolean foundRecords = false;

        for (var record : records) {
            if (record.offset() > endOffset) {
                return;
            }
            foundRecords = true;
            if (containsContractIdentifier(record.value(), contractID)) {
                Logger.info("Fetched message with Contract Identifier: " + record.value());
                try {
                    Logger.info("Started reprocessing the Record");
                    contractService.createOptimizedPayload(record.value());
                } catch (Exception e) {
                    Logger.error("Error during reprocessing", e);
                    throw new RuntimeException(e);
                }
            }
        }

        if (!foundRecords) {
            Logger.info("No more messages found in partition = " + topicPartition.partition());
            break;
        }
    }
}

private boolean containsContractIdentifier(String jsonString, String contractID) {
    var objectMapper = new ObjectMapper();
    try {
        var rootNode = objectMapper.readTree(jsonString);
        return findInNode(rootNode, contractID);
    } catch (Exception e) {
        throw new RuntimeException("Error parsing JSON", e);
    }
}

private boolean findInNode(JsonNode node, String contractID) {
    if (node.isObject()) {
        return findInObject(node, contractID);
    } else if (node.isArray()) {
        return findInArray(node, contractID);
    }
    return false;
}

private boolean findInObject(JsonNode node, String contractID) {
    for (var entry : iterable(node.fields())) {
        if (entry.getKey().equals("contractIdentifier") && entry.getValue().asText().equals(contractID)) {
            return true;
        }
        if (findInNode(entry.getValue(), contractID)) {
            return true;
        }
    }
    return false;
}

private boolean findInArray(JsonNode arrayNode, String contractID) {
    for (var element : arrayNode) {
        if (findInNode(element, contractID)) {
            return true;
        }
    }
    return false;
}

private static <T> Iterable<T> iterable(Iterator<T> iterator) {
    return () -> iterator;
}


}
