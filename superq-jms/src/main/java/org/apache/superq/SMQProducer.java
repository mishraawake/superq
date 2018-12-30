package org.apache.superq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

public class SMQProducer implements MessageProducer {

  private boolean disabledMessageId;
  private boolean disableMessageTimestamp;
  private int deliveryMode = 1;
  private int priority = 4;
  private long timeToLive = Long.MAX_VALUE;
  private long deliveryDelay = 0;
  private Destination destination;
  private long id;
  private AtomicLong messageIdStore = new AtomicLong(0);
  private Map<Long, AsyncProduceData> completionListenerMap = new HashMap<>();
  private SMQSession session;
  private int inflightMessages = 0;

  private boolean closing = false;

  public SMQProducer(SMQSession session){
    this.session = session;
  }

  @Override
  public void setDisableMessageID(boolean disabledMessageId) throws JMSException {
    this.disabledMessageId = disabledMessageId;
  }

  @Override
  public boolean getDisableMessageID() throws JMSException {
    return this.disabledMessageId;
  }

  @Override
  public void setDisableMessageTimestamp(boolean disableMessageTimestamp) throws JMSException {
    this.disableMessageTimestamp = disableMessageTimestamp;
  }

  @Override
  public boolean getDisableMessageTimestamp() throws JMSException {
    return this.disableMessageTimestamp;
  }

  @Override
  public void setDeliveryMode(int deliveryMode) throws JMSException {
    this.deliveryMode = deliveryMode;
  }

  @Override
  public int getDeliveryMode() throws JMSException {
    return this.deliveryMode;
  }

  @Override
  public void setPriority(int priority) throws JMSException {
    this.priority = priority;
  }

  @Override
  public int getPriority() throws JMSException {
    return this.priority;
  }

  @Override
  public void setTimeToLive(long timeToLive) throws JMSException {
    this.timeToLive = timeToLive;
  }

  @Override
  public long getTimeToLive() throws JMSException {
    return this.timeToLive;
  }

  @Override
  public void setDeliveryDelay(long deliveryDelay) throws JMSException {
    this.deliveryDelay = deliveryDelay;
  }

  @Override
  public long getDeliveryDelay() throws JMSException {
    return this.deliveryDelay;
  }

  @Override
  public Destination getDestination() throws JMSException {
    return this.destination;
  }

  public void setDestination(Destination destination) throws JMSException {
     this.destination = destination;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @Override
  public void close() throws JMSException {
    closing = true;
  }

  @Override
  public void send(Message message) throws JMSException {
    try {
      message.setJMSDestination(this.getDestination());
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendSynchronously(message);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  private JMSException getJMSException(IOException ioException){
    JMSException jmsException = new JMSException("Exception in sending the message");
    jmsException.setLinkedException(ioException);
    return jmsException;
  }

  @Override
  public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
    try {
      message.setJMSDestination(this.getDestination());
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendSynchronously(message);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  @Override
  public void send(Destination destination, Message message) throws JMSException {
    try {
      message.setJMSDestination(destination);
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendSynchronously(message);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  @Override
  public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
    try {
      message.setJMSDestination(destination);
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendSynchronously(message);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  @Override
  public void send(Message message, CompletionListener completionListener) throws JMSException {
    try {
      message.setJMSDestination(destination);
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendAsynchronously(message, completionListener);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  @Override
  public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
    try {
      message.setJMSDestination(destination);
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendAsynchronously(message, completionListener);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  @Override
  public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException {
    try {
      message.setJMSDestination(destination);
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendAsynchronously(message, completionListener);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  @Override
  public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
    try {

      message.setJMSDestination(destination);
      message.setJMSDeliveryMode(deliveryMode);
      message.setJMSPriority(priority);
      message.setJMSExpiration(timeToLive);
      sendAsynchronously(message, completionListener);
    } catch (IOException ioe){
      throw getJMSException(ioe);
    }
  }

  private void sendSynchronously(Message message) throws IOException, JMSException {
    if(closing){
      throw new JMSException("Producer is closing");
    }
    inflightMessages++;
    ((SMQMessage) message).setProducerId(this.getId());
    ((SMQMessage) message).setJmsMessageLongId(this.messageIdStore.incrementAndGet());
    message.setJMSDeliveryTime(System.currentTimeMillis());
    session.sendMessage((SMQMessage) message);
    inflightMessages--;
  }

  private void sendAsynchronously(Message message, CompletionListener completionListener) throws IOException, JMSException {
    inflightMessages++;
    if(closing){
      throw new JMSException("Producer is closing");
    }
    message.setJMSDeliveryTime(System.currentTimeMillis());
    ((SMQMessage) message).setProducerId(this.getId());
    ((SMQMessage) message).setJmsMessageLongId(this.messageIdStore.incrementAndGet());
    AsyncProduceData asyncProduceData = new AsyncProduceData();
    asyncProduceData.setCompletionListener(completionListener);
    asyncProduceData.setMessage(message);
    completionListenerMap.put(((SMQMessage) message).getJmsMessageLongId(),  asyncProduceData);
    session.sendAsynchronously((SMQMessage) message, completionListener);
  }

  public AsyncProduceData getMessageHandler(long messageId) {
    return completionListenerMap.get(messageId);
  }

  public void decrementInFlightMessage() {
    --inflightMessages;
  }
}
