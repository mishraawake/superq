package org.apache.superq.storage;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;

import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactoryImpl;

public class MessageStoreImpl implements MessageStore {

  FileDatabase<SMQMessage> fd;

  ConsumerMessageEnumerator consumerMessageEnumerator;

  public MessageStoreImpl(String qname, FileDatabase<SMQMessage> fd) throws IOException {
     this.fd = fd;
    consumerMessageEnumerator = new ConsumerMessageEnumerator(this);
  }

  @Override
  public SMQMessage getNextMessage() throws IOException {
    return consumerMessageEnumerator.nextElement();
  }

  @Override
  public MessageEnumerator browserEnumerator() throws IOException {
    return new MessageBrowserEnumerator(this);
  }


  @Override
  public boolean hasMoreMessage() throws IOException {
    return consumerMessageEnumerator.hasMoreElements();
  }

  @Override
  public void addMessage(SMQMessage smqMessage) throws IOException {
    fd.appendMessage(smqMessage);
  }

  @Override
  public void removeMessage(long messageId) throws IOException {
    fd.deleteMessage(messageId);
  }

  @Override
  public boolean doesSpaceLeft() {
    return false;
  }


  public static class MessageBrowserEnumerator implements MessageEnumerator {
    private int pageSize = 1000;
    List<SMQMessage> messageFetched = null;
    Long lastMessageId = null;
    MessageStoreImpl messageStore;
    SMQMessage message;
    boolean noMoreFetch = false;

    MessageBrowserEnumerator(MessageStoreImpl messageStore){
      this.messageStore = messageStore;
    }

    public boolean hasMoreElements() throws IOException {

      if(messageFetched == null || messageFetched.size() == 0) {
        if(noMoreFetch){
          return false;
        }
        if(lastMessageId !=null){
          lastMessageId++;
        }
        messageFetched = messageStore.fd.browseOldMessage(pageSize, lastMessageId);
        if(messageFetched.size() < pageSize ){
          SMQTextMessage textMessage = new SMQTextMessage();
          textMessage.setJmsMessageLongId(-1l);
          messageFetched.add(textMessage);
          noMoreFetch = true;
        }
      }
      message = messageFetched.get(0);
      messageFetched.remove(0);
      lastMessageId = message.getJmsMessageLongId();
      return true;
    }

    public SMQMessage nextElement() {
      return message;
    }
  }


  public static class ConsumerMessageEnumerator implements MessageEnumerator{
    private int pageSize = 10;
    List<SMQMessage> messageFetched = null;
    MessageStoreImpl messageStore;
    SMQMessage message;

    ConsumerMessageEnumerator(MessageStoreImpl messageStore){
      this.messageStore = messageStore;
    }

    public boolean hasMoreElements() throws IOException {
      if(messageFetched == null || messageFetched.size() == 0) {
        messageFetched = messageStore.fd.getOldMessage(pageSize);
        if(messageFetched.size() == 0){
          return false;
        }
      }
      message = messageFetched.get(0);
      messageFetched.remove(0);
      return true;
    }

    public SMQMessage nextElement() {
      return this.message;
    }
  }
}
