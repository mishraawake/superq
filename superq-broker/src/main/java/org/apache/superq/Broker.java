package org.apache.superq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactory;
import org.apache.superq.db.FileDatabaseFactoryImpl;
import org.apache.superq.db.InfoSizeableFactory;
import org.apache.superq.db.SizeableFactory;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.io.IOAsyncUtil;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;
import org.apache.superq.outgoing.SBConsumer;
import org.apache.superq.storage.IOMessageStoreFilter;
import org.apache.superq.storage.MessageStore;
import org.apache.superq.storage.MessageStoreImpl;
import org.apache.superq.storage.QueueStats;
import org.apache.superq.storage.SBQueue;
import org.apache.superq.storage.SBQueueDefault;
import org.apache.superq.storage.TransactionalStore;

public class Broker {

  CallbackExecutor callbackExecutor;
  Map<Integer, SBQueue<SMQMessage>> messageQueueMap = new ConcurrentHashMap<>();
  Map<String, ConnectionContext> allConnectionContext = new ConcurrentHashMap<>();
  private ThreadLocal<Boolean> callbackThread = new ThreadLocal<Boolean>(){
    @Override protected Boolean initialValue() {
      return false;
    }
  };
  Map<String, MessageStore<QueueInfo>> infoStores = new ConcurrentHashMap<>();
  Map<String, MessageStore<SMQMessage>> mainStores = new ConcurrentHashMap<>();
  volatile MessageStore<QueueInfo> mainInfo;
  MBeanServer mbs;


  public Broker(){
    callbackExecutor = new CallbackExecutor(callbackThread);
  }

  public void setMBeanServer(MBeanServer mbs){
    this.mbs = mbs;
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

  public MessageStore<SMQMessage> getMainMessageStore(String qname) throws IOException {
    if(!mainStores.containsKey(qname)){
      MessageStore<SMQMessage> messageStore = new IOMessageStoreFilter<>(new MessageStoreImpl<SMQMessage>
              (FileDatabaseFactoryImpl.getInstance().getOrInitializeMainDatabase(qname), this), this);
      mainStores.putIfAbsent(qname, messageStore);
    }
    return mainStores.get(qname);
  }

  public MessageStore<QueueInfo> getInfoMessageStore(String qname) throws IOException {
    if(!infoStores.containsKey(qname)){
      FileDatabaseFactory<QueueInfo> fileDatabaseFactory = FileDatabaseFactoryImpl.<QueueInfo>getInstance();
      MessageStore<QueueInfo> messageStore = new IOMessageStoreFilter<>(new MessageStoreImpl<QueueInfo>(fileDatabaseFactory.
              getInfoDatabase(qname, new InfoSizeableFactory<QueueInfo>()), this), this);
      infoStores.putIfAbsent(qname, messageStore);
    }
    return infoStores.get(qname);
  }

  public MessageStore<QueueInfo> getInfoMessageStore() throws IOException {
    if(mainInfo == null){
      mainInfo = new IOMessageStoreFilter<>(new MessageStoreImpl<QueueInfo>(FileDatabaseFactoryImpl.<QueueInfo>getInstance().
              getInfoDatabase(new InfoSizeableFactory<QueueInfo>()), this), this);
    }
    return mainInfo;
  }

  public synchronized QueueInfo getQInfo(String qname, MessageStore<QueueInfo> messageStore ) throws IOException, JMSException {
    while(messageStore.hasMoreMessage(null)){
      QueueInfo queueInfo = messageStore.getNextMessage();
      if(queueInfo.getQueueName().equals(qname)){
        return queueInfo;
      }
    }
    return null;
  }

  public synchronized QueueInfo getQInfo(int qindex, MessageStore<QueueInfo> messageStore ) throws IOException, JMSException {
    while(messageStore.hasMoreMessage(null)){
      QueueInfo queueInfo = messageStore.getNextMessage();
      if(queueInfo.getId() == qindex){
        return queueInfo;
      }
    }
    return null;
  }

  public void saveQueue(QueueInfo qinfo) throws JMSException, IOException {
    QueueInfo queueInfo = getQInfo(qinfo.getQueueName(), getInfoMessageStore());
    MessageStore<SMQMessage> mainDatabase = getMainMessageStore(qinfo.getQueueName());
    if(queueInfo == null){
      getInfoMessageStore().addMessage(qinfo);
      registerQueue(qinfo);
    } else {
      registerQueue(qinfo);
    }
  }

  public void registerQueue(QueueInfo qinfo) throws JMSException, IOException {
    SBQueue<SMQMessage> messageSBQueue = messageQueueMap.putIfAbsent(qinfo.getId(), new SBQueueDefault(
            this, qinfo, getMainMessageStore(qinfo.getQueueName())));
    if(messageSBQueue == null && mbs != null){
      registerQueueInMBean(messageQueueMap.get(qinfo.getId()));
    }
  }

  private void registerQueueInMBean(SBQueue<SMQMessage> smqMessageSBQueue) {
    try {
      QueueStats queueStats = new QueueStats(smqMessageSBQueue);
      ObjectName mxbeanName = new ObjectName("org.apache.superq:type=QueueStats");
      mbs.registerMBean(queueStats, mxbeanName);
    } catch (Exception e){
      e.printStackTrace();
    }
  }


  public QueueInfo getQueue(String qname) throws JMSException, IOException {
    return getQInfo(qname, getInfoMessageStore());
  }

  public SBQueue<SMQMessage> getQueue(int qindex) throws JMSException, IOException {
    return messageQueueMap.get(qindex);
  }

  public QueueInfo getQueueInfo(int qindex) throws JMSException, IOException {
    return messageQueueMap.get(qindex).getQInfo();
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
    queue.removeConsumer(consumerInfo);
  }

  public void removeConnection(String connectionId){
    allConnectionContext.remove(connectionId);
  }

  public void setAllConnectionContext(Map<String, ConnectionContext> allConnectionContext) {
    this.allConnectionContext = allConnectionContext;
  }

  public void pullMessage(PullMessage pullMessage) throws IOException {
    SBQueue<SMQMessage> queue = getSBQueue(pullMessage.getQid());
    queue.pullMessage(pullMessage);
  }
}
