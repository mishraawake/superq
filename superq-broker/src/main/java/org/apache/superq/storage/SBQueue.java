package org.apache.superq.storage;

import java.io.IOException;
import java.util.List;

import org.apache.superq.SMQMessage;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.outgoing.SBConsumer;

public interface SBQueue<M> {
  void acceptMessage(M message, SBProducerContext context) throws IOException;
  void acceptConsumer(SBConsumer<M> consumer);
  List<SBConsumer<M>> getConsumers();
  void start();
  void prepare();
  M pullMessage();
  void ackMessage(M m);
  boolean canAcceptMoreConsumer();
  QStatus getStatus();
  void addConstraint();
  List<QConstraint> getConstraints();
  void dispose();
  void onConsumerReadyForMessage(SBConsumer<SMQMessage> messageSBConsumer);
  void putMessageOnAnotherConsumerGroup(SBConsumer<SMQMessage> messageSBConsumer);
}
