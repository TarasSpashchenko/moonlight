package com.dataimport.moonlight.processing.service;

import com.dataimport.moonlight.processing.RecordParser;
import com.dataimport.moonlight.processing.cache.DummyReadThroughCache;
import com.dataimport.moonlight.processing.dao.DummySomeDataDao;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Random;

public class DummyParsedMarcProcessingServiceImpl implements ParsedMarcProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DummyParsedMarcProcessingServiceImpl.class);

  private final Vertx vertx;

  private final DummyReadThroughCache readThroughCache = new DummyReadThroughCache() {
    private final Random random = new Random(System.currentTimeMillis());

    private final DummySomeDataDao someDataDao = new DummySomeDataDao() {
      private final Random random = new Random(System.currentTimeMillis());

      @Override
      public Future<JsonObject> readSomeData(JsonObject someParams) {
        Promise<JsonObject> promise = Promise.promise();
        vertx.<JsonObject>executeBlocking(p -> {
          try {
            LOGGER.debug(Thread.currentThread().getName() + " - DAO public Future<JsonObject> readSomeData(JsonObject someParams)");
            Thread.sleep(random.nextInt(500));
            p.complete(new JsonObject());
          } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
          }
        }, false, ar -> {
          if (ar.succeeded()) {
            LOGGER.debug(Thread.currentThread().getName() + " - Done DAO public Future<JsonObject> readSomeData(JsonObject someParams)");
            promise.complete(ar.result());
          } else {
            promise.fail(ar.cause());
          }
        });
        return promise.future();
      }
    };

    @Override
    public Future<JsonObject> readSomeData(JsonObject someParams) {
      LOGGER.debug(Thread.currentThread().getName() + " - Cache public Future<JsonObject> readSomeData(JsonObject someParams)");
      return (random.nextInt(10000) > 9900) ? Future.succeededFuture(new JsonObject()) : someDataDao.readSomeData(someParams);
    }
  };

  private final RecordParser recordParser = new RecordParser() {

    @Override
    public JsonObject parseRecord(JsonObject rawRecord) {
      return JsonObject.mapFrom(rawRecord);
    }
  };

  public DummyParsedMarcProcessingServiceImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<JsonObject> processMarcRecord(JsonObject extendedMarcRecord) {
    LOGGER.debug(Thread.currentThread().getName() + " - public Future<JsonObject> processMarcRecord(JsonObject extendedMarcRecord)");

    return readThroughCache.readSomeData(new JsonObject()).compose(readData -> {
      JsonObject parsedRecord = recordParser.parseRecord(extendedMarcRecord);
      LOGGER.debug(Thread.currentThread().getName() + " - JsonObject parsedRecord = recordParser.parseRecord(extendedMarcRecord);");
      return Future.succeededFuture(new JsonObject().put("object1", parsedRecord).put("object2", parsedRecord));
    }, error -> Future.failedFuture(error.getCause()));
  }
}
