package org.apache.superq.storage;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;

import org.apache.superq.Broker;
import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.Serialization;
import org.apache.superq.Task;
import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactoryImpl;

public class MessageStoreImpl<M extends Serialization> implements MessageStore<M> {

  FileDatabase<M> fd;

  Broker broker;

  ConsumerMessageEnumerator<M> consumerMessageEnumerator;

  public MessageStoreImpl(FileDatabase<M> fd, Broker broker) throws IOException {
     this.fd = fd;
     this.broker = broker;
     consumerMessageEnumerator = new ConsumerMessageEnumerator<M>(this);
  }

  @Override
  public M getNextMessage() throws IOException {
    return consumerMessageEnumerator.nextElement();
  }

  @Override
  public MessageEnumerator<M> browserEnumerator(Class<M> className) throws IOException {
    if(className.equals(SMQMessage.class)){
      return new MessageBrowserEnumerator<M>(this, false);
    } else {
      return new MessageBrowserEnumerator<M>(this, true);
    }

  }


  @Override
  public boolean hasMoreMessage(Task afterIO) throws IOException {
    return consumerMessageEnumerator.hasMoreElements(afterIO);
  }

  @Override
  public void addMessage(M smqMessage) throws IOException {
    fd.appendMessage(smqMessage);
    consumerMessageEnumerator.hasMoreLements = true;
  }

  @Override
  public void removeMessage(long messageId) throws IOException {
    fd.deleteMessage(messageId);
  }

  @Override
  public boolean doesSpaceLeft() {
    return false;
  }

  @Override
  public MessageEnumerator<M> getMessageEnumerator(){
    return consumerMessageEnumerator;
  }


  public static class MessageBrowserEnumerator<M extends Serialization> implements MessageEnumerator<M> {
    private int pageSize = 1000;
    List<M> messageFetched = null;
    Long lastMessageId = null;
    MessageStoreImpl<M> messageStore;
    M message;
    boolean noMoreFetch = false;
    boolean all;

    MessageBrowserEnumerator(MessageStoreImpl<M> messageStore, boolean all){
      this.messageStore = messageStore;
      this.all = all;
    }

    public boolean hasMoreElements(Task task) throws IOException {
      if(all){
        return getAllInfo();
      }
      return getMoreMessages();
    }

    private boolean getAllInfo() throws IOException{
      if(messageFetched == null) {
        messageFetched = messageStore.fd.getAllMessage();
        if(messageFetched == null || messageFetched.size() == 0){
          return false;
        }
      }
      if(messageFetched.size() > 0){
        message = messageFetched.get(0);
        messageFetched.remove(0);
      } else {
        return false;
      }
      return true;
    }

    private boolean getMoreMessages() throws IOException {
      if(messageFetched == null || messageFetched.size() == 0) {
        if(noMoreFetch){
          return false;
        }
        if(lastMessageId !=null){
          lastMessageId++;
        }
        messageFetched = messageStore.fd.browseOldMessage(pageSize, lastMessageId);
        if(messageFetched.size() < pageSize ){
          noMoreFetch = true;
        }
      }
      message = messageFetched.get(0);
      messageFetched.remove(0);
      SMQMessage smqMessage = (SMQMessage)message;
      lastMessageId = smqMessage.getJmsMessageLongId();
      return true;
    }

    public M nextElement() {
      return message;
    }

    @Override
    public boolean moreMessage() {
      return false;
    }

    @Override
    public boolean beingLoaded() {
      return false;
    }

    @Override
    public int memorySize() {
      return 0;
    }
  }


  public static class ConsumerMessageEnumerator<M extends Serialization> implements MessageEnumerator<M>{
    private int pageSize = 10000;
    volatile List<M> messageFetched = null;
    MessageStoreImpl<M> messageStore;
    M message;
    volatile boolean hasMoreLements = true;
    int totalFetch = 0;
    volatile boolean beingLoaded = false;

    ConsumerMessageEnumerator(MessageStoreImpl<M> messageStore){
      this.messageStore = messageStore;
    }

    public boolean hasMoreElements(Task task) throws IOException {
      if(messageFetched == null || messageFetched.size() == 0) {
        if(task == null){
          messageFetched = messageStore.fd.getOldMessage(pageSize);
          if(messageFetched.size() == 0){
            return false;
          }
        } else {
        //  System.out.println(" hasMoreLements "+hasMoreLements+ " beingLoaded "+beingLoaded);
          if(hasMoreLements && !beingLoaded) {
            beingLoaded = true;
            if(beingLoaded)
            messageStore.broker.enqueueFileIo(new Task() {
              @Override
              public void perform() throws Exception {
              //  if (messageFetched != null)
                //  System.out.println("starting message fetch " + messageFetched.size());
                long stime = System.currentTimeMillis();
                List<M> messageFetchedRetuned = messageStore.fd.getOldMessage(pageSize);
              //  System.out.println("time in fetching " + (System.currentTimeMillis() - stime) + " batch " + pageSize + " result " + messageFetchedRetuned.size());
                if( !beingLoaded){
                  System.out.println("wrong value of being loaded");
                }
                beingLoaded = false;
                if (messageFetchedRetuned.size() == 0) {
                  hasMoreLements = false;
                }
                else {
                  totalFetch += messageFetchedRetuned.size();
                  messageFetched = messageFetchedRetuned;
                //  System.out.println("messageFetched " + totalFetch);
                }
              }
            }, task);
           // System.out.println(" trueing " + beingLoaded + " beingLoaded ");
          }
          return false;
        }
      }
      message = messageFetched.get(0);
      messageFetched.remove(0);
      return true;
    }

    public M nextElement() {
      return this.message;
    }

    @Override
    public boolean moreMessage() {
      return hasMoreLements;
    }

    @Override
    public boolean beingLoaded() {
      return beingLoaded;
    }

    @Override
    public int memorySize() {
      return messageFetched.size();
    }
  }
}
