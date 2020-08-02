package com.dataimport.moonlight;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    Promise<String> consumer1VerticlesDeployment = Promise.promise();
    Promise<String> consumer2VerticlesDeployment = Promise.promise();

    vertx.deployVerticle("com.dataimport.moonlight.consumers.RawMarcConsumerVerticle",
      new DeploymentOptions().setWorker(true).setInstances(15), consumer1VerticlesDeployment);

    vertx.deployVerticle("com.dataimport.moonlight.consumers.MarcProcessingCompletedConsumerVerticle",
      new DeploymentOptions().setWorker(true).setInstances(15), consumer2VerticlesDeployment);

    CompositeFuture.all(consumer1VerticlesDeployment.future(), consumer2VerticlesDeployment.future())
      .onSuccess(s -> startPromise.complete())
      .onFailure(startPromise::fail);
  }
}
