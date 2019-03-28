package org.apache.superq;

import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

public class SQQueueBrowser implements QueueBrowser {

  SMQSession session;
  long id;
  private BlockingQueue<SMQMessage> messages = new LinkedBlockingQueue<>();
  BrowserMessageEnumerator browserMessageEnumerator;
  private Queue queue;
  private String messageSelector;


  public SQQueueBrowser(String messageSelector){
    this.messageSelector = messageSelector;
    this.browserMessageEnumerator = new BrowserMessageEnumerator(this);
  }

  public void start() throws JMSException {
    BrowserInfo info = new BrowserInfo();
    info.setConnectionId(this.session.getConnection().getConnectionId());
    info.setSessionId(this.session.getId());
    info.setId(this.id);
    info.setQid(((SMQDestination)this.queue).getDestinationId());
    this.session.connection.sendAsync(info);
  }

  public long getId() {
    return id;
  }

  public void setSession(SMQSession smqSession){
    this.session = smqSession;
  }

  @Override
  public Queue getQueue() throws JMSException {
    return queue;
  }

  @Override
  public String getMessageSelector() throws JMSException {
    return this.messageSelector;
  }

  @Override
  public Enumeration getEnumeration() throws JMSException {
    return this.browserMessageEnumerator;
  }

  @Override
  public void close() throws JMSException {

  }

  public void setId(long id){
    this.id = id;
  }

  public void consumer(SMQMessage message){
    long messageId = message.getJmsMessageLongId();
    messages.add(message);
  }

  public void setQueue(Queue queue) {
    this.queue = queue;
  }

  private static class BrowserMessageEnumerator implements Enumeration<SMQMessage> {

    SQQueueBrowser browser;
    SMQMessage message;

    BrowserMessageEnumerator(SQQueueBrowser browser){
      this.browser = browser;
    }

    @Override
    public boolean hasMoreElements() {
      try {
        message = browser.messages.take();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
      if(message.getJmsMessageLongId() < 0){
        return false;
      }
      return true;
    }

    @Override
    public SMQMessage nextElement() {
      return message;
    }
  }
}
