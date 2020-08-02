package com.dataimport.moonlight.processing.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface DummySomeDataDao {
  Future<JsonObject> readSomeData(JsonObject someParams);
}
