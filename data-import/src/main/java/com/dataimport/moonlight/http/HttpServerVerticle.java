package com.dataimport.moonlight.http;

import com.dataimport.moonlight.processing.MarcStreamParser;
import com.dataimport.moonlight.processing.impl.MarcStreamParserImpl;
import com.dataimport.moonlight.processing.impl.MarcToKafkaWriteStreamWrapper;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.marc4j.marc.Record;

import java.util.HashMap;
import java.util.Map;

public class HttpServerVerticle extends AbstractVerticle {

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  //  private KafkaProducer<String, String> producer;
//  private KafkaAdminClient kafkaAdminClient;
  private HttpServer server;

  @Override
  public void start(Promise<Void> promise) {

//    Map<String, String> config = new HashMap<>();
//    config.put("bootstrap.servers", "localhost:9092");
//
//    kafkaAdminClient = KafkaAdminClient.create(vertx, config);

//    getKafkaProducer();

    Integer portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);

    server =
      vertx.createHttpServer(new HttpServerOptions().setHost("localhost").setPort(portNumber).setLogActivity(true));

    Router router = Router.router(vertx);
    router.post().handler(BodyHandler.create());
    router.post("/import/start").handler(this::startMarcImport);
    router.get("/import/testAsyncFile").handler(this::testAsyncFile);
    router.get("/import/testAsyncFileToKafka").handler(this::testAsyncFileToKafka);

    server
      .requestHandler(router)
      .listen(ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on portNumber " + portNumber);
          promise.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          promise.fail(ar.cause());
        }
      });
  }


  private void startMarcImport(RoutingContext context) {
    //TODO: retrieve filename from the request or somewhere else
    String fileName = "d:\\temp\\sample_bib_21.mrc";

    //TODO: call Source-Record-Manager to get topic names
//    AsyncFile x;
  }

  private void testAsyncFile(RoutingContext context) {
//    String fileName = "d:\\temp\\sample_bib_21.mrc";
    String fileName = "d:\\temp\\30_000_records_4.mrc";

    FileSystem fs = vertx.fileSystem();
    fs.open(fileName, new OpenOptions(), ar -> {
      AsyncFile asyncFile = ar.result();

      MarcStreamParser marcStreamParser = MarcStreamParserImpl.newMarcParser(vertx, asyncFile);
      marcStreamParser.pause();

      marcStreamParser.processAsynchronously(new DummyStdOutWriteStream(), par -> {
        System.out.println("marcStreamParser.processAsynchronously completed: par.succeeded(): " + par.succeeded());
      });

    });

    context.response().end("File processing has been started");
  }


  private void testAsyncFileToKafka(RoutingContext context) {
//    String fileName = "d:\\temp\\sample_bib_21.mrc";
    String fileName = "d:\\temp\\30_000_records_4.mrc";

    FileSystem fs = vertx.fileSystem();
    fs.open(fileName, new OpenOptions(), ar -> {
      AsyncFile asyncFile = ar.result();

      MarcStreamParser marcStreamParser = MarcStreamParserImpl.newMarcParser(vertx, asyncFile);
      marcStreamParser.pause();

      KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

      MarcToKafkaWriteStreamWrapper writeStreamWrapper = new MarcToKafkaWriteStreamWrapper(kafkaProducer, "MARC_READ", 15);
      System.out.println("About to start marcStreamParser.processAsynchronously");
      marcStreamParser.processAsynchronously(writeStreamWrapper, par -> {
        System.out.println("marcStreamParser.processAsynchronously completed: par.succeeded(): " + par.succeeded());
        writeStreamWrapper.end(ear -> {
          kafkaProducer.close(car -> System.out.println("kafkaProducer.close end handler car.succeeded(): " + car.succeeded()));
          System.out.println("writeStreamWrapper end handler ear.succeeded(): " + ear.succeeded());
        });
      });

    });

    context.response().end("File-to-Kafka processing has been started");
  }

  private KafkaProducer<String, String> getKafkaProducer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("metadata.max.age.ms", "30000");
    config.put("acks", "1");

    return KafkaProducer.create(vertx, config);
  }

  private class DummyStdOutWriteStream implements WriteStream<Record> {
    private int counter = 0;

    @Override
    public WriteStream<Record> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public WriteStream<Record> write(Record data) {
      return write(data, null);
    }

    @Override
    public WriteStream<Record> write(Record data, Handler<AsyncResult<Void>> handler) {
      System.out.println(Thread.currentThread().getName() + " Record: " + data.getLeader() + " " + (++counter));
      return this;
    }

    @Override
    public void end() {

    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public WriteStream<Record> setWriteQueueMaxSize(int maxSize) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public WriteStream<Record> drainHandler(@Nullable Handler<Void> handler) {
      return this;
    }
  }
}

