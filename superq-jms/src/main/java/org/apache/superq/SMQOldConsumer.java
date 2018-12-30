package org.apache.superq;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

public class SMQOldConsumer implements MessageConsumer {

  MessageListener messageListener;

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
    this.messageListener = messageListener;
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
}
