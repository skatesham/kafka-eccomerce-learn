package br.com.alura.ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable {
  private final KafkaConsumer<String, T> consumer;
  private final ConsumerFunction parse;

  KafkaService(final String groupId, final String topic, final ConsumerFunction parse, final Class<T> type, final Map<String,String> properties) {
    this(parse, groupId, type, properties);
    this.consumer.subscribe(Collections.singletonList(topic));
  }

  KafkaService(final String groupId, final Pattern topic, final ConsumerFunction parse, final Class<T> type, final Map<String,String> properties) {
    this(parse, groupId, type, properties);
    this.consumer.subscribe(topic);
  }

  private KafkaService(final ConsumerFunction parse, final String groupId, final Class<T> type, final Map<String, String> properties) {
    this.parse = parse;
    this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
  }

  void run() {
    while (true) {
      final var records = this.consumer.poll(Duration.ofMillis(100));
      if (!records.isEmpty()) {
        System.out.println("Encontrei " + records.count() + " registros");
        for (final var record : records) {
          this.parse.consume(record);
        }
      }
    }
  }

  private Properties getProperties(final Class<T> type, final String groupId, final Map<String, String> overrideProperties) {
    final var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
    properties.putAll(overrideProperties);
    return properties;
  }

  @Override
  public void close() {
    this.consumer.close();
  }
}
