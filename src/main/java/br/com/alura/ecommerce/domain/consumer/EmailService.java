package br.com.alura.ecommerce.domain.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.infrastructure.KafkaService;

public class EmailService {

	public static void main(final String[] args) {
		final EmailService emailService = new EmailService();
		try (final KafkaService service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL",
				emailService::parse)) {
			service.run();
		}
	}

	private void parse(final ConsumerRecord<String, String> record) {
		System.out.println("------------------------------------------");
		System.out.println("Send email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (final InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		System.out.println("Email sent");
	}

}
