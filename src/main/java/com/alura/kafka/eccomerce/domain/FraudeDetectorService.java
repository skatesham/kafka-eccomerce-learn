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
package com.alura.kafka.eccomerce.domain;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @version $Id$
 */
public class FraudeDetectorService
{

    public static void main(final String[] args) throws InterruptedException, ExecutionException
    {
        System.out.println("Kafka System!");

        // System
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties());
        final String topic = "ECOMMERCE_NEW_ORDER";
        final List<String> topics = Collections.singletonList(topic);
        kafkaConsumer.subscribe(topics);
        while (true) {
            final ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(100));

            if (!poll.isEmpty()) {
                System.out.println("ECOMMERCE_NEW_ORDER: Found " + poll.count() + "!");
                // kafkaConsumer.close();
                Thread.sleep(500);
                for (final ConsumerRecord<String, String> consumerRecord : poll) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println(consumerRecord.key() + " :: " + consumerRecord.value());
                    System.out
                        .println("Partition: " + consumerRecord.partition() + ", offset:" + consumerRecord.offset());
                    Thread.sleep(2000);
                }
            }
        }
    }

    /**
     * @return Kafka Configuration
     */
    private static Properties properties()
    {
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudeDetectorService.class.getSimpleName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,
            FraudeDetectorService.class.getSimpleName() + "_" + UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

}
