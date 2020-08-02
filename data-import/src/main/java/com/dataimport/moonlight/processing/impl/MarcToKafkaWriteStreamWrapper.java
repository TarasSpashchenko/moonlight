package com.dataimport.moonlight.processing.impl;

import com.dataimport.moonlight.processing.RawToJsonMarcConverter;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.marc4j.marc.Record;

public class MarcToKafkaWriteStreamWrapper implements WriteStream<Record> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcToKafkaWriteStreamWrapper.class);

  private final RawToJsonMarcConverter converter = RawToJsonMarcConverterSimpleImpl.getInstance();

  private final WriteStream<KafkaProducerRecord<String, String>> delegate;
  private final String topicName;
  private final int maxPartitionNum;
  private int messageCounter = 0;

  public MarcToKafkaWriteStreamWrapper(WriteStream<KafkaProducerRecord<String, String>> delegate, String topicName, int maxPartitionNum) {
    super();
    this.delegate = delegate;
    this.topicName = topicName;
    this.maxPartitionNum = maxPartitionNum;
  }

  @Override
  public WriteStream<Record> exceptionHandler(Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public WriteStream<Record> write(Record data) {
    return write(data, null);
  }

  @Override
  public WriteStream<Record> write(Record data, Handler<AsyncResult<Void>> handler) {
    JsonObject jsonMarcRecord = converter.convert(data);

    String key = String.valueOf(++messageCounter % maxPartitionNum);

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, jsonMarcRecord.toString());
    delegate.write(record, handler);

    return this;
  }

  @Override
  public void end() {
    delegate.end();
    LOGGER.debug(Thread.currentThread().getName() + " - public void end()");
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    delegate.end(ar -> {
      LOGGER.debug(Thread.currentThread().getName() + " - Messages sent: " + messageCounter);
      handler.handle(ar);
    });
    LOGGER.debug(Thread.currentThread().getName() + " - public void end(Handler<AsyncResult<Void>> handler)");
  }

  @Override
  public WriteStream<Record> setWriteQueueMaxSize(int maxSize) {
    delegate.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return delegate.writeQueueFull();
  }

  @Override
  public WriteStream<Record> drainHandler(@Nullable Handler<Void> handler) {
    delegate.drainHandler(handler);
    return this;
  }
}
