package com.dataimport.moonlight.http;

import com.dataimport.moonlight.processing.MarcStreamParser;
import com.dataimport.moonlight.processing.impl.MarcStreamParserImpl;
import com.dataimport.moonlight.processing.impl.MarcToKafkaWriteStreamWrapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

public class HttpServerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";

  private HttpServer server;

  @Override
  public void start(Promise<Void> promise) {

    Integer portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);

    server =
      vertx.createHttpServer(new HttpServerOptions().setHost("localhost").setPort(portNumber).setLogActivity(true));

    Router router = Router.router(vertx);
    router.post().handler(BodyHandler.create());
    router.get("/import/start").handler(this::startMarcImport);

    server
      .requestHandler(router)
      .listen(ar -> {
        if (ar.succeeded()) {
          LOGGER.info(Thread.currentThread().getName() + " - HTTP server running on portNumber " + portNumber);
          promise.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          promise.fail(ar.cause());
        }
      });
  }


  private void startMarcImport(RoutingContext context) {
//    String fileName = "d:\\temp\\sample_bib_21.mrc";
    String fileName = "./30_000_records_4.mrc";

    FileSystem fs = vertx.fileSystem();
    fs.open(fileName, new OpenOptions(), ar -> {
      AsyncFile asyncFile = ar.result();

      MarcStreamParser marcStreamParser = MarcStreamParserImpl.newMarcParser(vertx, asyncFile);
      marcStreamParser.pause();

      KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

      MarcToKafkaWriteStreamWrapper writeStreamWrapper = new MarcToKafkaWriteStreamWrapper(kafkaProducer, "MARC_READ", 15);
      LOGGER.info(Thread.currentThread().getName() + " - About to start marcStreamParser.processAsynchronously");
      marcStreamParser.processAsynchronously(writeStreamWrapper, par -> {
        LOGGER.info(Thread.currentThread().getName() + " - marcStreamParser.processAsynchronously completed: par.succeeded(): " + par.succeeded());
        writeStreamWrapper.end(ear -> {
          kafkaProducer.close(car -> LOGGER.debug(Thread.currentThread().getName() + " - kafkaProducer.close end handler car.succeeded(): " + car.succeeded()));
          LOGGER.info(Thread.currentThread().getName() + " - writeStreamWrapper end handler ear.succeeded(): " + ear.succeeded());
        });
      });

    });

    context.response().end("File-to-Kafka processing has been started");
  }

  private KafkaProducer<String, String> getKafkaProducer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "kafka:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("metadata.max.age.ms", "30000");
    config.put("acks", "1");

    return KafkaProducer.create(vertx, config);
  }

}

