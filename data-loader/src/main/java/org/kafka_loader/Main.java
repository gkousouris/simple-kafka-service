import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class Main {
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
  private static final String TOPIC = "people_data_topic";

  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      JsonNode rootArray = objectMapper.readTree(new File("random-people-data.json")).get("ctRoot");
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
}