package org.apache.superq.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.superq.QueueInfo;
import org.apache.superq.SMQMessage;
import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactoryImpl;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TransactionalStoreImpl implements TransactionalStore {

  Set<TransactionSync> syncSet = new HashSet<>();
  List<SMQMessage> messageList = new ArrayList<>();

  @Override
  public void addSynchrounization(TransactionSync sync) {
    syncSet.add(sync);
  }

  @Override
  public void commit() throws JMSException, IOException {
    for(SMQMessage message : messageList){
        FileDatabase<SMQMessage> messageDatabase = FileDatabaseFactoryImpl.getInstance().getOrInitializeMainDatabase(((QueueInfo) message.getJMSDestination()).getQueueName());
        messageDatabase.appendMessage(message);
    }
    for(TransactionSync sync : syncSet){
      sync.afterCommit();
    }
    syncSet = new HashSet<>();
    messageList = new ArrayList<>();
  }

  @Override
  public void rollback() {
    for(TransactionSync sync : syncSet){
      sync.afterRollback();
    }
    syncSet = new HashSet<>();
    messageList = new ArrayList<>();
  }

  @Override
  public SMQMessage getNextMessage() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public boolean hasMoreMessage() {
     throw new NotImplementedException();
  }

  @Override
  public void addMessage(SMQMessage smqMessage) throws IOException {
    messageList.add(smqMessage);
  }

  @Override
  public boolean doesSpaceLeft() {
    throw new NotImplementedException();
  }

}
