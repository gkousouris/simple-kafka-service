package org.kafka_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.InterruptException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaConsumerService {

  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
  private static final String GROUP_ID = "kafka_consumer_group";
  private KafkaConsumer<String, String> consumer;
  private static final ObjectMapper objectMapper = new ObjectMapper();


  public KafkaConsumerService() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    this.consumer = new KafkaConsumer<>(properties);
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  public String getMessages(String topicName, int offset, int count) {
    // Get partitions for the given topic
    List<TopicPartition> partitions = getPartitions(topicName);
    if (partitions.isEmpty()) {
      return "Topic: " + topicName + " not found.";
    }

    consumer.assign(partitions);
    Map<Integer, List<Map<String, Object>>> partitionMessages = new HashMap<>();

    for (TopicPartition partition : partitions) {
      // If offset is -1, fetch the last `count` messages from the topic
      if (offset == -1) {
        // Seek to the latest message minus count
        consumer.seekToEnd(Collections.singleton(partition));
        long lastOffset = consumer.position(partition);
        consumer.seek(partition, Math.max(0, (int)(lastOffset - count)));
      } else {
        consumer.seek(partition, offset);
      }

      List<Map<String, Object>> messages = new ArrayList<>();
      int messagesConsumed = 0;

      // Poll messages until 'count' messages are consumed
      while (messagesConsumed < count) {
        var records = consumer.poll(java.time.Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records.records(partition)) {
          Map<String, Object> messageData = new HashMap<>();
          messageData.put("partition", record.partition());
          messageData.put("offset", record.offset());
          messageData.put("message", record.value());
          messages.add(messageData);
          messagesConsumed++;

          if (messagesConsumed >= count) {
            break;
          }
        }
      }
      partitionMessages.put(partition.partition(), messages);
    }

    // Aggregate all results in a map the response map
    Map<String, Object> response = new HashMap<>();
    response.put("topic", topicName);
    if (offset == -1)
      response.put("offset", "latest");
    else
      response.put("offset", offset);
    response.put("count", count);
    response.put("partitions", partitionMessages);

    try {
      // Return the structured response as a JSON string with pretty-printing
      return objectMapper.writeValueAsString(response);
    } catch (Exception e) {
      return "Failed to serialize response.";
    }
  }

  private List<TopicPartition> getPartitions(String topicName) {
    List<TopicPartition> partitions = new ArrayList<>();

    try {
      // Create an AdminClient to retrieve topic metadata
      Properties adminProps = new Properties();
      adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
      AdminClient adminClient = KafkaAdminClient.create(adminProps);

      // Get the partitions for the topic
      Map<String, org.apache.kafka.clients.admin.TopicDescription> topicDescriptionMap = adminClient.describeTopics(List.of(topicName)).all().get();
      org.apache.kafka.clients.admin.TopicDescription topicDescription = topicDescriptionMap.get(topicName);

      if (topicDescription != null) {
        topicDescription.partitions().forEach(partitionInfo ->
            partitions.add(new TopicPartition(topicName, partitionInfo.partition()))
        );
      }

      adminClient.close();
    } catch (Exception e) {
      // Return an empty list if any exceptions occur
      return new ArrayList<>();
    }
    return partitions;
  }
}