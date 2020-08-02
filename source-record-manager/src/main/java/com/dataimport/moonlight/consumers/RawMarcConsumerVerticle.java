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
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RawMarcConsumerVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawMarcConsumerVerticle.class);

  private final static AtomicInteger indexer = new AtomicInteger();

  private final static AtomicInteger loadSensor = new AtomicInteger(0);

  private final static AtomicInteger counter = new AtomicInteger(0);

  private final int id = indexer.getAndIncrement();

  private RawMarcProcessingService rawMarcProcessingService;
  private KafkaConsumer<String, String> kafkaRawMarcsConsumer;
  private KafkaProducer<String, String> kafkaParsedMarcsProducer;


  public void start(Promise<Void> startPromise) {
    rawMarcProcessingService = new DummyRawMarcProcessingServiceImpl(vertx);
    kafkaParsedMarcsProducer = getKafkaParsedMarcsProducer();
    kafkaRawMarcsConsumer = getKafkaRawMarcsConsumer();

    kafkaRawMarcsConsumer.handler(record -> {
      LOGGER.info(Thread.currentThread().getName() + " - <" + id + "> - a Record has been received. key: " + record.key() + " loadSensor: " + loadSensor.incrementAndGet() + " counter: " + counter.incrementAndGet());
      rawMarcProcessingService.processMarcRecord(new JsonObject(record.value())).onComplete(ar -> {

        KafkaProducerRecord<String, String> parsedRecord =
          KafkaProducerRecord.create("MARC_PARSED", record.key(), ar.result().toString());

        kafkaParsedMarcsProducer.send(parsedRecord, sar -> {
          LOGGER.debug(Thread.currentThread().getName() + " - a parsed record has been sent");
          Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
          TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
          OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, null);
          offsets.put(topicPartition, offsetAndMetadata);
          LOGGER.debug(Thread.currentThread().getName() + " - commiting message offset...");
          kafkaRawMarcsConsumer.commit(offsets, completionHandler -> {
            loadSensor.decrementAndGet();
            LOGGER.debug(Thread.currentThread().getName() + " - ...>Committed for offsetAndMetadata: " + offsetAndMetadata);
          });
        });
      });
    });

    kafkaRawMarcsConsumer.subscribe("MARC_READ", ar -> {
        if (ar.succeeded()) {
          LOGGER.info(Thread.currentThread().getName() + " - ***>Consumer created:" + kafkaRawMarcsConsumer + ", deploymentID: " + id);
          startPromise.complete();
        } else {
          ar.cause().printStackTrace();
          startPromise.fail(ar.cause());
        }

      }
    );

  }

  private KafkaConsumer<String, String> getKafkaRawMarcsConsumer() {
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

  private KafkaProducer<String, String> getKafkaParsedMarcsProducer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "kafka:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("metadata.max.age.ms", "30000");
    config.put("acks", "1");

    return KafkaProducer.createShared(vertx, "ParsedMarcsProducer", config);
  }

  public void stop(Promise<Void> stopPromise) {
    kafkaParsedMarcsProducer.close(
      pcar -> kafkaRawMarcsConsumer.unsubscribe(
        ar -> kafkaRawMarcsConsumer.close(
          car -> LOGGER.info(Thread.currentThread().getName() + " - ***>Consumer closed:" + kafkaRawMarcsConsumer + ", deploymentID: " + id))));
  }
}
