package br.com.alura.ecommerce.domain.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.infrastructure.KafkaService;

public class FraudDetectorService {

	public static void main(final String[] args) {
		final FraudDetectorService fraudDetectorService = new FraudDetectorService();
		try (final KafkaService service = new KafkaService(FraudDetectorService.class.getSimpleName(),
				"ECOMMERCE_NEW_ORDER", fraudDetectorService::parse)) {
			service.run();
		}
	}

	private void parse(final ConsumerRecord<String, String> record) {
		System.out.println("------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(5000);
		} catch (final InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}

}
