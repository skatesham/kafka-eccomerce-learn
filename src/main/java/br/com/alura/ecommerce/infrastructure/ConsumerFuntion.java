package br.com.alura.ecommerce.infrastructure;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ConsumerFuntion {

	void consume(ConsumerRecord<String, String> record);

}
