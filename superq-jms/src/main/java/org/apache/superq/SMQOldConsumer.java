package org.apache.superq;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

public class SMQOldConsumer implements MessageConsumer {

  MessageListener messageListener;
  SMQSession session;
  long id;
  String messageSelector;
  QueueInfo queue;
  volatile private SMQMessage currentReceiveMessage = null;
  Object messageMutex = new Object();
  volatile private boolean started;
  BlockingQueue<SMQMessage> messageQueue = new LinkedBlockingQueue<>();

  public SMQOldConsumer(String messageSelector){
    this.messageSelector = messageSelector;
  }

  public void start(boolean sync) throws JMSException {
    ConsumerInfo info = new ConsumerInfo();
    info.setConnectionId(this.session.getConnection().getConnectionId());
    info.setSessionId(this.session.getId());
    info.setId(this.id);
    info.setQid(((SMQDestination)this.queue).getDestinationId());
    info.setAsync(sync);
    this.session.connection.sendAsync(info);
    started = true;
  }

  @Override
  public String getMessageSelector() throws JMSException {
    return messageSelector;
  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    return this.messageListener;
  }

  @Override
  public void setMessageListener(MessageListener listener) throws JMSException {
    checkStarted();
    this.messageListener = listener;
    this.start(true);
  }

  private void checkStarted() {
      if(started){
        throw new RuntimeException("Could not set messageListener once started");
      }
  }

  private void beforeReceive() throws JMSException {
    if(started && messageListener != null){
      throw new RuntimeException("Cannot receive on consumer which has message consumer");
    } else if(!started){
      this.start(false);
    }
  }

  @Override
  public Message receive() throws JMSException {
    beforeReceive();
    SMQMessage message;
    // send a pull command
    if(currentReceiveMessage != null){
      message = currentReceiveMessage;
      currentReceiveMessage = null;
    } else {
      sendPullRequest();
      while(currentReceiveMessage == null){
        synchronized (messageMutex) {
          try {
            messageMutex.wait();
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      // send a command to broker and then wait for
      message = currentReceiveMessage;
      currentReceiveMessage = null;
    }

    return message;
  }

  private void sendPullRequest() throws JMSException {
    PullMessage pullMessage = new PullMessage();
    pullMessage.setConnectionId(this.session.getConnection().getConnectionId());
    pullMessage.setSessionId(this.session.getId());
    pullMessage.setId(this.id);
    pullMessage.setQid(((SMQDestination)this.queue).getDestinationId());
    this.session.connection.sendAsync(pullMessage);
  }

  @Override
  public Message receive(long timeout) throws JMSException {
    beforeReceive();
    SMQMessage message;
    if(currentReceiveMessage != null){
      message = currentReceiveMessage;
      currentReceiveMessage = null;
    } else {
      sendPullRequest();
      long totalTime = 0;
      while(totalTime < timeout && currentReceiveMessage == null){
        synchronized (messageMutex) {
          try {
            long stime = System.currentTimeMillis();
            messageMutex.wait(timeout - totalTime);
            totalTime += (System.currentTimeMillis() - stime);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      message = currentReceiveMessage;
      currentReceiveMessage = null;
    }
    return message;
  }

  @Override
  public Message receiveNoWait() throws JMSException {
    beforeReceive();
    SMQMessage message = null;
    if(currentReceiveMessage != null){
      message = currentReceiveMessage;
      currentReceiveMessage = null;
    } else {
      sendPullRequest();
    }
    return message;
  }

  @Override
  public void close() throws JMSException {

  }

  public SMQSession getSession() {
    return session;
  }

  public void setSession(SMQSession session) {
    this.session = session;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public QueueInfo getQueue() {
    return queue;
  }

  public void setQueue(QueueInfo queue) {
    this.queue = queue;
  }

  public void receiveForMessageListener(SMQMessage message){
    messageQueue.add(message);
  }

  public SMQMessage getNextMessage(){
    return messageQueue.remove();
  }

  public void receiveInternal(SMQMessage message) {
    currentReceiveMessage = message;
    synchronized (messageMutex) {
      messageMutex.notifyAll();
    }
  }

  public void handleOnMessage(SMQMessage message) throws JMSException {
    this.getMessageListener().onMessage(message);
    acknowledge(message);
  }

  private void acknowledge(SMQMessage message) throws JMSException {
    ConsumerAck consumerAck = new ConsumerAck();
    consumerAck.setMessageId(message.getJmsMessageLongId());
    consumerAck.setConnectionId(message.getConnectionId());
    consumerAck.setSessionId(session.getId());
    consumerAck.setQid(this.getQueue().getId());
    consumerAck.setId(message.getConsumerId());
    session.connection.sendAsync(consumerAck);
  }
}
