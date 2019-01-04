package org.apache.superq.db;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.SMQMessage;
import org.apache.superq.storage.MessageEnumerator;
import org.apache.superq.storage.MessageStoreImpl;
import org.junit.Assert;
import org.junit.Test;

public class MessageStoreImplTest extends AbstractTest {

  @Test
  public void messageGet() throws IOException, JMSException {
    final int totalMessage = 1000;
    createMessage(totalMessage);
    MessageStoreImpl store = new MessageStoreImpl(qname, fileDatabase);
    int numberOfMessages = 0;
    while(store.hasMoreMessage()){
      SMQMessage message = store.getNextMessage();
      if(message != null){
        ++numberOfMessages;
      }
    }
    Assert.assertEquals(numberOfMessages, totalMessage);
    createMessage(10);
    numberOfMessages = 0;
    while(store.hasMoreMessage()){
      SMQMessage message = store.getNextMessage();
      if(message != null){
        ++numberOfMessages;
      }
    }

    Assert.assertEquals(numberOfMessages, 10);
  }

  @Test
  public void messageBrowse() throws IOException, JMSException {
    final int totalMessage = 100;
    createMessage(totalMessage);
    MessageStoreImpl store = new MessageStoreImpl(qname, fileDatabase);
    int numberOfMessages = 0;
    MessageEnumerator enumerator = store.browserEnumerator();
    while(enumerator.hasMoreElements()){
      SMQMessage message = enumerator.nextElement();
      if(message != null){
        ++numberOfMessages;
      }
    }
    Assert.assertEquals(numberOfMessages, totalMessage + 1);
    numberOfMessages = 0;
    enumerator = store.browserEnumerator();
    while(enumerator.hasMoreElements()){
      SMQMessage message = enumerator.nextElement();
      if(message != null){
        ++numberOfMessages;
      }
    }

    Assert.assertEquals(numberOfMessages, totalMessage +  1);
  }

}
