package com.dataimport.moonlight.processing.cache;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface DummyReadThroughCache {
  Future<JsonObject> readSomeData(JsonObject someParams);
}
