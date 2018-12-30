package org.apache.superq;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactoryImpl;
import org.apache.superq.db.InfoSizeableFactory;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.io.IOAsyncUtil;
import org.apache.superq.storage.SBQueue;
import org.apache.superq.storage.SBQueueDefault;
import org.apache.superq.storage.TransactionalStore;

public class Broker {

  CallbackExecutor callbackExecutor;
  Map<Long, SBQueue<SMQMessage>> messageQueueMap = new ConcurrentHashMap<>();
  TransactionalStore transactionalStore;


  public Broker(){
    callbackExecutor = new CallbackExecutor();
  }

  public void start(){
    new Thread(callbackExecutor, "Callback executor thread").start();
  }


  public void putOnCallback(Task task){
    callbackExecutor.addTask(task);
  }

  public QueueInfo getConsumerInfo(String qname, FileDatabase<QueueInfo> fd ) throws IOException, JMSException {
    List<QueueInfo> infos = fd.getAllMessage();
    for (int index = 0; index < infos.size(); index++) {
      QueueInfo info = infos.get(index);
      if(info.getQueueName().equals(qname)){
        return info;
      }
    }
    return null;
  }

  public void saveQueue(QueueInfo serialization) throws JMSException, IOException {
    FileDatabase<QueueInfo> fd = FileDatabaseFactoryImpl.getInstance().
            getInfoDatabase(serialization.getQueueName(), new InfoSizeableFactory());
    QueueInfo queueInfo = getConsumerInfo(serialization.getQueueName(), fd);
    if(queueInfo == null){
      fd.appendMessage(serialization);
      messageQueueMap.putIfAbsent(queueInfo.getId(), new SBQueueDefault(this, queueInfo, transactionalStore));
    }
  }


  public QueueInfo getQueue(String qname) throws JMSException, IOException {
    FileDatabase<QueueInfo> fd = FileDatabaseFactoryImpl.getInstance().
            getInfoDatabase(qname, new InfoSizeableFactory());
    QueueInfo queueInfo = getConsumerInfo(qname, fd);
    return queueInfo;
  }

  public void enqueueFileIo(Task task, Task callback) {
    IOAsyncUtil.enqueueFileIo(task, new Task(){
      @Override
      public void perform() throws Exception {
        putOnCallback(callback);
      }
    } );
  }

  private SBQueue<SMQMessage> getQueue(Long destinationId){
    return messageQueueMap.get(destinationId);
  }

  public void appendMessage(SMQMessage message, SBProducerContext sbProducerContext) throws JMSException, IOException {
    Destination destination = message.getJMSDestination();
    if(destination instanceof QueueInfo){
      getQueue(((QueueInfo)destination).getId()).acceptMessage(message, sbProducerContext);
    }
  }

  public void startTransaction() {

  }
}
