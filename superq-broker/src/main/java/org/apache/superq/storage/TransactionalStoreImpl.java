package org.apache.superq.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.superq.Broker;
import org.apache.superq.CommitTransaction;
import org.apache.superq.QueueInfo;
import org.apache.superq.SMQMessage;
import org.apache.superq.Task;
import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactoryImpl;
import org.apache.superq.network.ConnectionContext;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TransactionalStoreImpl implements TransactionalStore {

  Set<TransactionSync> syncSet = new HashSet<>();
  List<SMQMessage> messageList = new ArrayList<>();
  Broker broker;

  public TransactionalStoreImpl(Broker broker){
    this.broker = broker;
  }

  @Override
  public void addSynchrounization(TransactionSync sync) {
    syncSet.add(sync);
  }

  @Override
  public void commit(ConnectionContext connectionContext, CommitTransaction commitTransaction) throws JMSException, IOException {

    broker.enqueueFileIo(new Task() {
      @Override
      public void perform() throws Exception {
        long stime = System.currentTimeMillis();
        for (SMQMessage message : messageList) {
          String qname = ((QueueInfo) message.getJMSDestination()).getQueueName();
          MessageStore<SMQMessage> essageStore = broker.getMainMessageStore(qname);
          essageStore.addMessage(message);
        }
        System.out.println("time in commiting " + (System.currentTimeMillis() - stime) + " of "+messageList.size());
      }
    }, new Task() {
      @Override
      public void perform() throws Exception {
        for(TransactionSync sync : syncSet){
          sync.afterCommit();
        }
        syncSet = new HashSet<>();
        messageList = new ArrayList<>();
        connectionContext.sendAsyncPacket(commitTransaction);
      }
    });
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
  public void addMessage(SMQMessage smqMessage) throws IOException {
    messageList.add(smqMessage);
  }

  @Override
  public boolean doesSpaceLeft() {
    throw new NotImplementedException();
  }

}
