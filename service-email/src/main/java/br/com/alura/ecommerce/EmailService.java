package br.com.alura.ecommerce;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

  public static void main(final String[] args) {
    final var emailService = new EmailService();
    try (var service = new KafkaService<>(EmailService.class.getSimpleName(),
        "ECOMMERCE_SEND_EMAIL", emailService::parse, String.class, Map.of())) {
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
