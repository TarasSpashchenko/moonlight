package com.dataimport.moonlight.processing.impl;

import com.dataimport.moonlight.processing.MarcStreamParser;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.Record;

public class MarcStreamParserImpl implements MarcStreamParser {

  private final Vertx vertx;
  private final ReadStream<Buffer> stream;

  private boolean asyncProcessingActive = false;
  private boolean demand = false;
  private boolean streamEnded;

  private Handler<Record> eventHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;

  private final VertxBufferInputStream bufferInputStream;
  private final MarcReader marcReader;

  private boolean terminated;
  private boolean terminationOnErrorRequested;
  private boolean terminationOnErrorCompleted;

  private Throwable terminationOnErrorCause;

  private static final Logger LOGGER = LoggerFactory.getLogger(MarcStreamParserImpl.class);

  public static MarcStreamParser newMarcParser(Vertx vertx, ReadStream<Buffer> stream) {
    return new MarcStreamParserImpl(vertx, stream);
  }

  private MarcStreamParserImpl(Vertx vertx, ReadStream<Buffer> stream) {
    super();
    this.vertx = vertx;
    this.stream = stream;
    bufferInputStream = new VertxBufferInputStream();
    marcReader = new MarcPermissiveStreamReader(bufferInputStream, true, true, "BESTGUESS");
  }

  @Override
  public void processAsynchronously(WriteStream<Record> destination, Handler<AsyncResult<Void>> completionHandler) {
    pause();
    this.pipeTo(destination, completionHandler);
  }

  @Override
  public void handle(Buffer buffer) {
    bufferInputStream.populate(buffer);

    int remainingBuffersCapacity = bufferInputStream.remainingBuffersCapacity();
    //TODO: get rid of magic numbers
    if (remainingBuffersCapacity < 10) {
      ReadStream<Buffer> s = stream;
      if (s != null) {
        s.pause();
        LOGGER.debug("Source stream is paused in handle(Buffer buffer).");
      }
    }
  }

  @Override
  public MarcStreamParser exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public MarcStreamParser handler(Handler<Record> handler) {
    eventHandler = handler;
    if (stream != null) {
      if (handler != null) {
        stream.endHandler(v -> {
          streamEnded = true;
          endStream();
        });
        stream.exceptionHandler(err -> {
          if (exceptionHandler != null) {
            exceptionHandler.handle(err);
          }
        });
        stream.handler(this);
      } else {
        stream.handler(null);
        stream.endHandler(null);
        stream.exceptionHandler(null);
      }
    }
    return this;
  }

  @Override
  public MarcStreamParser pause() {
    LOGGER.debug("MarcStreamParser.pause()");
    if (demand) {
      demand = false;
      if (!streamEnded) {
        ReadStream<Buffer> s = stream;
        if (s != null) {
          s.pause();
          LOGGER.debug("Source stream is paused in pause().");
        }
      }
    }
    return this;
  }

  @Override
  public MarcStreamParser fetch(long amount) {
    if (terminated || terminationOnErrorRequested) {
      LOGGER.warn("MarcStreamParser.fetch(long amount) - MarcStreamParser is already terminated...");
      return this;
    }
    if (!demand) {
      demand = true;
      LOGGER.debug("MarcStreamParser.fetch(long amount)");

      doProcessAsynchronously();
      if (!streamEnded) {
        ReadStream<Buffer> s = stream;
        if (s != null) {
          s.resume();
          LOGGER.debug("Source stream is resumed.");
        }
      }
    }
    return this;
  }

  @Override
  public MarcStreamParser resume() {
    if (terminated || terminationOnErrorRequested) {
      LOGGER.warn("MarcStreamParser.resume() - MarcStreamParser is already terminated...");
      return this;
    }

    if (eventHandler == null) {
      LOGGER.warn("MarcStreamParser.resume() - eventHandler is null, resume skipped...");
      return this;
    }

    boolean pausedNow = !demand;
    if (LOGGER.isDebugEnabled() && pausedNow) {
      LOGGER.debug("MarcStreamParser.resume()");
    }
    return pausedNow ? fetch(Long.MAX_VALUE) : this;
  }

  @Override
  public MarcStreamParser endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  @Override
  public void terminateOnError(Throwable terminatedCause) {
    LOGGER.warn("MarcStreamParser.terminateOnError(Throwable terminatedCause): " + terminatedCause);
    terminatedCause.printStackTrace();
    if (!terminationOnErrorRequested) {
      this.terminationOnErrorCause = terminatedCause;
      terminationOnErrorCompleted = false;
      terminationOnErrorRequested = true;

      //We don't need to process stream so Just push to doTerminate()!
      doProcessAsynchronously();
    }
  }

  @Override
  public boolean isTerminatedOnError() {
    return terminationOnErrorRequested;
  }

  @Override
  public Throwable getTerminationOnErrorCause() {
    return terminationOnErrorCause;
  }

  public boolean isTerminated() {
    return terminated;
  }

  private void endStream() {
    bufferInputStream.end();
    LOGGER.debug("MarcStreamParser.endStream() - completed.");
  }

  private void end() {
    if (!terminated) {
      terminated = true;
      LOGGER.debug("MarcStreamParser.end() - starting...");
      try {
        Handler<Void> handler = endHandler;
        if (handler != null) {
          handler.handle(null);
        }
      } finally {
        bufferInputStream.close();
        LOGGER.debug("MarcStreamParser.end() - completed.");
      }
    }
  }

  private void abort() {
    LOGGER.debug("MarcStreamParser.abort()");
    demand = false;
    streamEnded = true;

    ReadStream<Buffer> s = stream;
    if (s != null) {
      s.pause();
      LOGGER.debug("Source stream is paused in abort().");
    }
  }

  private void doTerminate() {
    if (!terminationOnErrorCompleted) {
      LOGGER.debug("MarcStreamParser.doTerminate() - starting...");
      abort();
      end();

      Handler<Throwable> exHandler = this.exceptionHandler;
      if (exHandler != null) {
        exHandler.handle(terminationOnErrorCause);
      }

      terminationOnErrorCompleted = true;
      LOGGER.debug("MarcStreamParser.doTerminate() - completed.");
    }
  }


  private void doProcessAsynchronously() {
    if (terminated) {
      LOGGER.warn("MarcStreamParser is already terminated...");
    } else if (terminationOnErrorRequested) {
      doTerminate();
    } else {
      if (asyncProcessingActive) {
        return;
      }
      asyncProcessingActive = true;

      vertx.<Record>executeBlocking(promise -> {
        try {
          if (marcReader.hasNext()) {
            promise.complete(marcReader.next());
          } else {
            promise.complete(null);
          }
        } catch (Throwable t) {
          promise.fail(t);
        }
      }, true, ar -> {
        asyncProcessingActive = false;
        if (ar.succeeded()) {
          Record result = ar.result();
          if (result != null) {
            Handler<Record> localEventHandler = eventHandler;
            if (localEventHandler != null) {
              localEventHandler.handle(result);

              if (!streamEnded && demand) {
                int remainingBuffersCapacity = bufferInputStream.remainingBuffersCapacity();
                //TODO: get rid of magic numbers
                if (remainingBuffersCapacity > 40) {
                  ReadStream<Buffer> s = stream;
                  if (s != null) {
                    s.resume();
                    LOGGER.debug("Source stream is resumed in asyncHandler.");
                  }
                }
              }
              if (demand) {
                doProcessAsynchronously();
              }
            } else {
              LOGGER.error("Event handler is null!");
              end();
            }
          } else {
            end();
          }
        } else {
          terminateOnError(ar.cause());
        }
      });
    }
  }

}
