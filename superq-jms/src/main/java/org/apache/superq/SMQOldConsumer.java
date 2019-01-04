package org.apache.superq;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

public class SMQOldConsumer implements MessageConsumer {

  MessageListener messageListener;
  SMQSession session;
  long id;
  QueueInfo queue;

  public SMQOldConsumer(){

  }

  public void start() throws JMSException {
    ConsumerInfo info = new ConsumerInfo();
    info.setConnectionId(this.session.getConnection().getConnectionId());
    info.setSessionId(this.session.getId());
    info.setId(this.id);
    info.setQid(((SMQDestination)this.queue).getDestinationId());
    this.session.connection.sendAsync(info);
  }

  @Override
  public String getMessageSelector() throws JMSException {
    return null;
  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    return this.messageListener;
  }

  @Override
  public void setMessageListener(MessageListener listener) throws JMSException {
    this.messageListener = listener;
    this.start();
  }

  @Override
  public Message receive() throws JMSException {
    return null;
  }

  @Override
  public Message receive(long timeout) throws JMSException {
    return null;
  }

  @Override
  public Message receiveNoWait() throws JMSException {
    return null;
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
}
