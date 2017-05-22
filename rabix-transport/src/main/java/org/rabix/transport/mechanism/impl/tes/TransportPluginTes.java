package org.rabix.transport.mechanism.impl.tes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.configuration.Configuration;
import org.rabix.common.json.BeanSerializer;
import org.rabix.common.json.processor.BeanProcessorException;
import org.rabix.transport.mechanism.TransportPlugin;
import org.rabix.transport.mechanism.TransportPluginException;
import org.rabix.transport.mechanism.TransportPluginType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportPluginTes implements TransportPlugin<TransportQueueTes> {

  private static final Logger logger = LoggerFactory.getLogger(TransportPluginTes.class);

  private ConcurrentMap<TransportQueueTes, Receiver<?>> receivers = new ConcurrentHashMap<>();

  private ExecutorService receiverThreadPool = Executors.newCachedThreadPool();

  public TransportPluginTes(Configuration configuration) throws TransportPluginException {}

  @Override
  public <T> ResultPair<T> send(TransportQueueTes queue, T entity) {

    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target("http://localhost:8000/v1/jobs");

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
    Response response = invocationBuilder.post(Entity.entity(BeanSerializer.serializeFull(entity), MediaType.APPLICATION_JSON));


    ResultPair<T> success = ResultPair.success(response.readEntity(TESJobId.class).getValue());
    return success;
  }

  @Override
  public TransportPluginType getType() {
    return TransportPluginType.LOCAL;
  }

  @Override
  public <T> void startReceiver(TransportQueueTes sourceQueue, Class<T> clazz, ReceiveCallback<T> receiveCallback, ErrorCallback errorCallback) {
    final Receiver<T> receiver = new Receiver<>(clazz, receiveCallback, errorCallback, sourceQueue);
    receivers.put(sourceQueue, receiver);
    receiverThreadPool.submit(new Runnable() {
      @Override
      public void run() {
        receiver.start();
      }
    });
  }

  @Override
  public void stopReceiver(TransportQueueTes queue) {
    Receiver<?> receiver = receivers.get(queue);
    if (receiver != null) {
      receiver.stop();
      receivers.remove(queue);
    }
  }

  private class Receiver<T> {

    private Class<T> clazz;
    private ReceiveCallback<T> callback;
    private ErrorCallback errorCallback;

    private TransportQueueTes queue;

    private volatile boolean isStopped = false;

    public Receiver(Class<T> clazz, ReceiveCallback<T> callback, ErrorCallback errorCallback, TransportQueueTes queue) {
      this.clazz = clazz;
      this.callback = callback;
      this.queue = queue;
    }

    void start() {
      try {
        WebTarget webTarget = ClientBuilder.newClient().target("http://localhost:8000/v1/jobs/" + queue.getQueue());
        while (!isStopped) {
          Thread.sleep(500);
          Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
          Response response = invocationBuilder.get();

          callback.handleReceive(response.readEntity(clazz));
        }
      } catch (BeanProcessorException e) {
        logger.error("Failed to deserialize message payload", e);
        errorCallback.handleError(e);
      } catch (TransportPluginException e) {
        logger.error("Failed to handle receive", e);
        errorCallback.handleError(e);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    void stop() {
      isStopped = true;
    }

  }

}
