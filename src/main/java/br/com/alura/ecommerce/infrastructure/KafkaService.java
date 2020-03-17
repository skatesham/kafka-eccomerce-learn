package br.com.alura.ecommerce.infrastructure;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService implements Closeable {

	private final String groupId;
	private final ConsumerFuntion parse;
	private final KafkaConsumer<String, String> consumer;

	public KafkaService(final String groupId, final String topic, final ConsumerFuntion parse) {
		this.parse = parse;
		this.groupId = groupId;
		this.consumer = new KafkaConsumer<String, String>(properties());
		this.consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(final String groupId, final Pattern topic, final ConsumerFuntion parse) {
		this.parse = parse;
		this.groupId = groupId;
		this.consumer = new KafkaConsumer<String, String>(properties());
		this.consumer.subscribe(topic);
	}

	public void run() {
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

	private Properties properties() {
		final var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		return properties;
	}

	@Override
	public void close() {
		this.consumer.close();
	}

}
