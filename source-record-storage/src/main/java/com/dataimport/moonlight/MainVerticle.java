package com.dataimport.moonlight;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    Promise<String> consumerVerticlesDeployment = Promise.promise();

    vertx.deployVerticle("com.dataimport.moonlight.consumers.ParsedMarcConsumerVerticle",
      new DeploymentOptions().setWorker(true).setInstances(15), consumerVerticlesDeployment);

    consumerVerticlesDeployment.future().onSuccess(s -> startPromise.complete()).onFailure(startPromise::fail);
  }
}
