package org.rabix.transport.mechanism.impl.tes;

import org.rabix.transport.mechanism.TransportQueue;

public class TransportQueueTes implements TransportQueue {

  private final String queue;
  
  public TransportQueueTes(String queue) {
    this.queue = queue;
  }
  
  public String getQueue() {
    return queue;
  }
}
