package com.dataimport.moonlight.consumers;

import com.dataimport.moonlight.processing.service.DummyParsedMarcProcessingServiceImpl;
import com.dataimport.moonlight.processing.service.ParsedMarcProcessingService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ParsedMarcConsumerVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParsedMarcConsumerVerticle.class);

  private final static AtomicInteger indexer = new AtomicInteger();

  private final static AtomicInteger loadSensor = new AtomicInteger(0);

  private final static AtomicInteger counter = new AtomicInteger(0);

  private final int id = indexer.getAndIncrement();

  private ParsedMarcProcessingService parsedMarcProcessingService;
  private KafkaConsumer<String, String> kafkaParsedMarcsConsumer;
  private KafkaProducer<String, String> kafkaStoredMarcsProducer;

  public void start(Promise<Void> startPromise) {
    this.parsedMarcProcessingService = new DummyParsedMarcProcessingServiceImpl(vertx);
    kafkaStoredMarcsProducer = getKafkaStoredMarcsProducer();
    kafkaParsedMarcsConsumer = getKafkaParsedMarcsConsumer();

    kafkaParsedMarcsConsumer.handler(record -> {
      LOGGER.info(Thread.currentThread().getName() + " - <" + id + "> - a Record has been received. key: " + record.key() + " loadSensor: " + loadSensor.incrementAndGet() + " counter: " + counter.incrementAndGet());
      parsedMarcProcessingService.processMarcRecord(new JsonObject(record.value())).onComplete(ar -> {

        KafkaProducerRecord<String, String> parsedRecord =
          KafkaProducerRecord.create("MARC_STORED", record.key(), ar.result().toString());

        kafkaStoredMarcsProducer.send(parsedRecord, sar -> {
          LOGGER.debug(Thread.currentThread().getName() + " - a parsed record has been sent");
          Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
          TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
          OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, null);
          offsets.put(topicPartition, offsetAndMetadata);
          LOGGER.debug(Thread.currentThread().getName() + " - commiting message offset...");
          kafkaParsedMarcsConsumer.commit(offsets, completionHandler -> {
            loadSensor.decrementAndGet();
            LOGGER.debug(Thread.currentThread().getName() + " - ...>Committed for offsetAndMetadata: " + offsetAndMetadata);
          });
        });
      });
    });

    kafkaParsedMarcsConsumer.subscribe("MARC_PARSED", ar -> {
        if (ar.succeeded()) {
          LOGGER.debug(Thread.currentThread().getName() + " - ***>Consumer created:" + kafkaParsedMarcsConsumer + ", deploymentID: " + id);
          startPromise.complete();
        } else {
          ar.cause().printStackTrace();
          startPromise.fail(ar.cause());
        }

      }
    );

  }

  private KafkaConsumer<String, String> getKafkaParsedMarcsConsumer() {
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

  private KafkaProducer<String, String> getKafkaStoredMarcsProducer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "kafka:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("metadata.max.age.ms", "30000");
    config.put("acks", "1");

    return KafkaProducer.createShared(vertx, "StoredMarcsProducer", config);
  }

  public void stop(Promise<Void> stopPromise) {
    kafkaStoredMarcsProducer.close(
      pcar -> kafkaParsedMarcsConsumer.unsubscribe(
        ar -> kafkaParsedMarcsConsumer.close(
          car -> LOGGER.info(Thread.currentThread().getName() + " - ***>Consumer closed:" + kafkaParsedMarcsConsumer + ", deploymentID: " + id))));
  }
}
