package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

  public static void main(final String[] args) throws ExecutionException, InterruptedException {
    try (var orderDispatcher = new KafkaDispatcher<Order>()) {
      try (var emailDispatcher = new KafkaDispatcher<String>()) {
        for (var i = 0; i < 10; i++) {

          final var userId = UUID.randomUUID().toString();
          final var orderId = UUID.randomUUID().toString();
          final var amount = new BigDecimal(Math.random() * 5000 + 1);

          final var order = new Order(userId, orderId, amount);
          orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

          final var email = "Thank you for your order! We are processing your order!";
          emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
        }
      }
    }
  }

}
