package com.dataimport.moonlight.processing.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface StoredMarcProcessingService {
  Future<JsonObject> processMarcRecord(JsonObject extendedMarcRecord);
}
