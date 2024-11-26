package org.kafka_service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerApp {
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
  private static final String TOPIC = "random_people_data_topic";
  private static final String PEOPLE_FILE_PATH = "random-people-data.json";


  public static void main(String[] args) {

    KafkaProducer<String, String> producer = getKafkaProducer();

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      JsonNode rootArray = objectMapper.readTree(new File(PEOPLE_FILE_PATH)).get("ctRoot");
      for (JsonNode personData : rootArray) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, personData.get("_id").toString(), personData.toString());
        producer.send(record);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    producer.flush();
    producer.close();
  }

  private static KafkaProducer<String, String> getKafkaProducer() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    return new KafkaProducer<>(properties);
  }
}