package com.dataimport.moonlight.processing.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface RawMarcProcessingService {
  Future<JsonObject> processMarcRecord(JsonObject extendedMarcRecord);
}
