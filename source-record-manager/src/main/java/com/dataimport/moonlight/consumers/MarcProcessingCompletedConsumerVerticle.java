package com.dataimport.moonlight.consumers;

import com.dataimport.moonlight.processing.service.DummyRawMarcProcessingServiceImpl;
import com.dataimport.moonlight.processing.service.RawMarcProcessingService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MarcProcessingCompletedConsumerVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcProcessingCompletedConsumerVerticle.class);

  private final static AtomicInteger indexer = new AtomicInteger();

  private final static AtomicInteger loadSensor = new AtomicInteger(0);

  private final static AtomicInteger counter = new AtomicInteger(0);

  private final int id = indexer.getAndIncrement();

  private RawMarcProcessingService rawMarcProcessingService;
  private KafkaConsumer<String, String> kafkaMarcProcessingCompletedConsumer;


  public void start(Promise<Void> startPromise) {
    rawMarcProcessingService = new DummyRawMarcProcessingServiceImpl(vertx);
    kafkaMarcProcessingCompletedConsumer = getKafkaMarcProcessingCompletedConsumer();

    kafkaMarcProcessingCompletedConsumer.handler(record -> {
      LOGGER.info(Thread.currentThread().getName() + " - <" + id + "> - a MarcProcessingCompleted Record has been received. key: " + record.key() + " loadSensor: " + loadSensor.incrementAndGet() + " counter: " + counter.incrementAndGet());
      rawMarcProcessingService.processMarcRecord(new JsonObject(record.value())).onComplete(ar -> {

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, null);
        offsets.put(topicPartition, offsetAndMetadata);
        LOGGER.debug(Thread.currentThread().getName() + " - commiting message offset...");
        kafkaMarcProcessingCompletedConsumer.commit(offsets, completionHandler -> {
          loadSensor.decrementAndGet();
          LOGGER.debug(Thread.currentThread().getName() + " - ...>Committed for offsetAndMetadata: " + offsetAndMetadata);
        });
      });
    });

    kafkaMarcProcessingCompletedConsumer.subscribe("MARC_PROCESSING_COMLETTED", ar -> {
        if (ar.succeeded()) {
          LOGGER.info(Thread.currentThread().getName() + " - ***>Consumer created:" + kafkaMarcProcessingCompletedConsumer + ", deploymentID: " + id);
          startPromise.complete();
        } else {
          ar.cause().printStackTrace();
          startPromise.fail(ar.cause());
        }

      }
    );

  }

  private KafkaConsumer<String, String> getKafkaMarcProcessingCompletedConsumer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "kafka:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "consumer_group_1");
//    config.put("client.id", "consumer_1");
    config.put("metadata.max.age.ms", "30000");
    config.put("enable.auto.commit", "false");
    config.put("auto.offset.reset", "earliest");

    return KafkaConsumer.create(vertx, config);
  }


  public void stop(Promise<Void> stopPromise) {
    kafkaMarcProcessingCompletedConsumer.unsubscribe(
      ar -> kafkaMarcProcessingCompletedConsumer.close(
        car -> LOGGER.info(Thread.currentThread().getName() + " - ***>Consumer closed:" + kafkaMarcProcessingCompletedConsumer + ", deploymentID: " + id)));
  }
}
