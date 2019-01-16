package org.apache.superq.storage;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import org.apache.superq.BrowserInfo;
import org.apache.superq.ConsumerAck;
import org.apache.superq.ConsumerInfo;
import org.apache.superq.PullMessage;
import org.apache.superq.QueueInfo;
import org.apache.superq.SMQMessage;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;
import org.apache.superq.outgoing.SBConsumer;

public interface SBQueue<M> {
  void acceptMessage(M message, SBProducerContext context) throws IOException;
  void acceptConsumer(SBConsumer<M> consumer) throws IOException;
  void removeConsumer(ConsumerInfo consumerInfo) throws IOException;
  void acceptBrowser(BrowserInfo browserInfo, SessionContext connectionContext) throws IOException;
  Queue<SBConsumer<M>> getConsumers();
  void start();
  void prepare();
  M pullMessage(PullMessage pullMessage) throws IOException;
  void ackMessage(ConsumerAck m) throws IOException;
  boolean canAcceptMoreConsumer();
  QStatus getStatus();
  void addConstraint();
  List<QConstraint> getConstraints();
  void dispose();
  void onConsumerReadyForMessage(SBConsumer<SMQMessage> messageSBConsumer);
  void putMessageOnAnotherConsumerGroup(SBConsumer<SMQMessage> messageSBConsumer);
  QueueInfo getQInfo();
}
