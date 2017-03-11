package org.rabix.executor.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.configuration.Configuration;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.rabix.transport.backend.Backend;
import org.rabix.transport.backend.impl.BackendRabbitMQ;
import org.rabix.transport.backend.impl.BackendRabbitMQ.EngineConfiguration;

import com.google.inject.Inject;

public class BackendRegister {

  private Configuration configuration;

  @Inject
  public BackendRegister(Configuration configuration) {
    this.configuration = configuration;
  }

  public Backend start() throws ExecutorException {
    try {
      return registerBackend();
    } catch (Exception e) {
      throw new ExecutorException("Failed to register executor to the Engine", e);
    }
  }

  private BackendRabbitMQ registerBackend() {
    String engineHost = configuration.getString("engine.url");
    Integer enginePort = configuration.getInteger("engine.port", null);

    Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));
    WebTarget webTarget = client.target(engineHost + ":" + enginePort + "/v0/engine/backends");

    String rabbitHost = configuration.getString("rabbitmq.host");
    String rabbitEngineExchange = configuration.getString("rabbitmq.engine.exchange");
    String rabbitEngineExchangeType = configuration.getString("rabbitmq.engine.exchangeType");
    String rabbitEngineReceiveRoutingKey = configuration.getString("rabbitmq.engine.receiveRoutingKey");
    String rabbitEngineHeartbeatRoutingKey = configuration.getString("rabbitmq.engine.heartbeatRoutingKey");

    EngineConfiguration engineConfiguration = new EngineConfiguration(rabbitEngineExchange, rabbitEngineExchangeType, rabbitEngineReceiveRoutingKey,
        rabbitEngineHeartbeatRoutingKey);
    BackendRabbitMQ backendRabbitMQ = new BackendRabbitMQ(null, rabbitHost, engineConfiguration, null);

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
    Response response = invocationBuilder.post(Entity.entity(backendRabbitMQ, MediaType.APPLICATION_JSON));
    return response.readEntity(BackendRabbitMQ.class);
  }
}
