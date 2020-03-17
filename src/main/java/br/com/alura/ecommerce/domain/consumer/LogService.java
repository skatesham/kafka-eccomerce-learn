package br.com.alura.ecommerce.domain.consumer;

import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.infrastructure.KafkaService;

public class LogService {

	public static void main(final String[] args) {
		final LogService logService = new LogService();
		try (KafkaService service = new KafkaService(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"),
				logService::parse)) {
			service.run();
		}
	}

	private void parse(final ConsumerRecord<String, String> record) {
		System.out.println("------------------------------------------");
		System.out.println("LOG: " + record.topic());
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
	}

}
