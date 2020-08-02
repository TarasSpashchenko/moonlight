package com.dataimport.moonlight.processing;

import io.vertx.core.json.JsonObject;

public interface RecordParser {

  JsonObject parseRecord(JsonObject rawRecord);
}
