/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.alura.kafka.eccomerce;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @version $Id$
 */
public class EcommerceApplication
{

    static final Callback callback = (data, ex) -> {
        if (ex != null) {
            ex.printStackTrace();
            return;
        }
        System.out.println("Success on send for " + data.topic() + ":::partition " + data.partition() + "/ offset "
            + data.offset() + " timestamp: " + data.timestamp());

    };

    /**
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws InterruptedException, ExecutionException
    {

        // System
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties());

        for (int i = 0; i < 100; i++) {
            final String key = UUID.randomUUID().toString();
            sendNewOrder(producer, key);
            sendEmailRequest(producer, key);
        }
    }

    /**
     * @return Kafka Configuration
     */
    private static Properties properties()
    {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static void sendNewOrder(final KafkaProducer<String, String> producer, final String key)
        throws InterruptedException, ExecutionException
    {

        final String value = "{content: " + UUID.randomUUID().toString() + "}";

        final ProducerRecord<String, String> record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
        producer.send(record, callback).get();
    }

    private static void sendEmailRequest(final KafkaProducer<String, String> producer, final String key)
        throws InterruptedException, ExecutionException
    {

        final String email =
            "{content: 'Thank you for the order! We are processing the order!', email: 'sham.vinicuis@gmail.com'}";
        final ProducerRecord<String, String> recordEmail = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
        producer.send(recordEmail, callback).get();
    }

}
