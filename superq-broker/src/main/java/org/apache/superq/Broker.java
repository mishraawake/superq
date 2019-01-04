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
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;
import org.apache.superq.outgoing.SBConsumer;
import org.apache.superq.storage.SBQueue;
import org.apache.superq.storage.SBQueueDefault;
import org.apache.superq.storage.TransactionalStore;

public class Broker {

  CallbackExecutor callbackExecutor;
  Map<Integer, SBQueue<SMQMessage>> messageQueueMap = new ConcurrentHashMap<>();
  Map<Integer, QueueInfo> qinfoCache = new ConcurrentHashMap<>();
  Map<String, ConnectionContext> allConnectionContext = new ConcurrentHashMap<>();
  private ThreadLocal<Boolean> callbackThread = new ThreadLocal<Boolean>(){
    @Override protected Boolean initialValue() {
      return false;
    }
  };

  public Broker(){
    callbackExecutor = new CallbackExecutor(callbackThread);
  }

  public void start(){
    new Thread(callbackExecutor, "Callback executor thread").start();
  }


  public boolean isCallbackThread(){
    return callbackThread.get();
  }

  public void putOnCallback(Task task){
    callbackExecutor.addTask(task);
  }

  public synchronized QueueInfo getQInfo(String qname, FileDatabase<QueueInfo> fd ) throws IOException, JMSException {
    List<QueueInfo> infos = fd.getAllMessage();
    for (int index = 0; index < infos.size(); index++) {
      QueueInfo info = infos.get(index);
      if(info.getQueueName().equals(qname)){
        return info;
      }
    }
    return null;
  }

  public synchronized QueueInfo getQInfo(int qindex, FileDatabase<QueueInfo> fd ) throws IOException, JMSException {
    List<QueueInfo> infos = fd.getAllMessage();
    for (int index = 0; index < infos.size(); index++) {
      QueueInfo info = infos.get(index);
      if(info.getId() == qindex){
        return info;
      }
    }
    return null;
  }

  public void saveQueue(QueueInfo qinfo) throws JMSException, IOException {
    FileDatabase<QueueInfo> fd = FileDatabaseFactoryImpl.getInstance().
            getInfoDatabase( new InfoSizeableFactory());
    QueueInfo queueInfo = getQInfo(qinfo.getQueueName(), fd);
    FileDatabase<SMQMessage> mainDatabase = FileDatabaseFactoryImpl.getInstance().getOrInitializeMainDatabase(qinfo.getQueueName());
    if(queueInfo == null){
      fd.appendMessage(qinfo);
      qinfoCache.putIfAbsent(qinfo.getId(), qinfo);
      messageQueueMap.putIfAbsent(qinfo.getId(), new SBQueueDefault(this, qinfo, mainDatabase));
    } else {
      messageQueueMap.putIfAbsent(qinfo.getId(), new SBQueueDefault(this, qinfo, mainDatabase));
    }
  }

  public void registerQueue(QueueInfo qinfo) throws JMSException, IOException {
    FileDatabase<SMQMessage> mainDatabase = FileDatabaseFactoryImpl.getInstance().getOrInitializeMainDatabase(qinfo.getQueueName());
    messageQueueMap.putIfAbsent(qinfo.getId(), new SBQueueDefault(this, qinfo, mainDatabase));
  }


  public QueueInfo getQueue(String qname) throws JMSException, IOException {
    FileDatabase<QueueInfo> fd = FileDatabaseFactoryImpl.getInstance().
            getInfoDatabase(qname, new InfoSizeableFactory());
    QueueInfo queueInfo = getQInfo(qname, fd);
    return queueInfo;
  }

  public SBQueue<SMQMessage> getQueue(int qindex) throws JMSException, IOException {
    return messageQueueMap.get(qindex);
  }

  public QueueInfo getQueueInfo(int qindex) throws JMSException, IOException {
    if(qinfoCache.containsKey(qindex)){
      return qinfoCache.get(qindex);
    }
    FileDatabase<QueueInfo> fd = FileDatabaseFactoryImpl.getInstance().
            getInfoDatabase(new InfoSizeableFactory());
    QueueInfo queueInfo = getQInfo(qindex, fd);
    qinfoCache.putIfAbsent(qindex, queueInfo);
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

  private SBQueue<SMQMessage> getSBQueue(int destinationId){
    return messageQueueMap.get(destinationId);
  }

  public void appendMessage(SMQMessage message, SBProducerContext sbProducerContext) throws JMSException, IOException {
    Destination destination = message.getJMSDestination();
    if(destination instanceof QueueInfo){
      getQueue(((QueueInfo)destination).getId()).acceptMessage(message, sbProducerContext);
    }
  }

  public void addBrowser(BrowserInfo browserInfo, SessionContext sessionContext) throws IOException, JMSException {
    SBQueue<SMQMessage> queue = getSBQueue(browserInfo.getQid());
    queue.acceptBrowser(browserInfo, sessionContext);
  }

  public void addConsumer(SBConsumer<SMQMessage> sbConsumer) throws IOException {
    ConsumerInfo consumerInfo = sbConsumer.getConsumerInfo();
    SBQueue<SMQMessage> queue = getSBQueue(consumerInfo.getQid());
    queue.acceptConsumer(sbConsumer);
  }

  public void removeConsumer(ConsumerInfo consumerInfo) throws IOException {
    SBQueue<SMQMessage> queue = getSBQueue(consumerInfo.getQid());
    queue.removeConsumer(consumerInfo.getId());
  }

  public void removeConnection(String connectionId){
    allConnectionContext.remove(connectionId);
  }

  public void setAllConnectionContext(Map<String, ConnectionContext> allConnectionContext) {
    this.allConnectionContext = allConnectionContext;
  }
}
