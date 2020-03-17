package br.com.alura.ecommerce.infrastructure;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDispetcher implements Closeable {

	private final KafkaProducer<String, String> producer;

	public KafkaDispetcher() {
		this.producer = new KafkaProducer<String, String>(properties());
	}

	private static Properties properties() {
		final var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

	public void send(final String topic, final String key, final String value)
			throws InterruptedException, ExecutionException {
		final var record = new ProducerRecord<>(topic, key, value);
		final Callback callback = (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "
					+ data.offset() + "/ timestamp " + data.timestamp());
		};
		this.producer.send(record, callback).get();

	}

	@Override
	public void close() {
		this.producer.close();
	}
}
