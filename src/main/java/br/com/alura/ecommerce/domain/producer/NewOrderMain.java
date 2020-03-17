package br.com.alura.ecommerce.domain.producer;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.alura.ecommerce.infrastructure.KafkaDispetcher;

public class NewOrderMain {

	public static void main(final String[] args) throws InterruptedException, ExecutionException {
		try (final KafkaDispetcher dispetcher = new KafkaDispetcher()) {

			for (var i = 0; i < 10; i++) {
				final var key = UUID.randomUUID().toString();

				final var value = key + ",67523,1234";
				dispetcher.send("ECOMMERCE_NEW_ORDER", key, value);

				final var email = "Thank you for your order! We are processing your order!";
				dispetcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
		}
	}

}
