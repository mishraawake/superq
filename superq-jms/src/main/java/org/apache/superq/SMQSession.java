package org.apache.superq;

import java.io.Closeable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

public class SMQSession implements javax.jms.Session {

  private boolean isTransacted;
  private int acknowledgeMode;
  private int sessionMode;
  SMQConnection connection;
  private long id;
  Map<Long, SMQProducer> producerMap = new HashMap<>();
  Map<Long, AutoCloseable> consumerMap = new HashMap<>();
  AtomicLong producerIdStore = new AtomicLong(0);
  AtomicLong consumerIdStore = new AtomicLong(0);
  AtomicLong transactioStore = new AtomicLong(0);
  AtomicInteger queueId = new AtomicInteger(0);
  private final int timeout = 5000;
  private Long currentTrId  = null;

  public SMQSession(boolean isTransacted, int acknowledgeMode) throws JMSException {
    this.isTransacted = isTransacted;
    this.acknowledgeMode = acknowledgeMode;
  }

  public void initialize() throws JMSException {
    this.connection.sendSync(createSessionInfo());
    if(this.connection.isStarted()){
      this.start();
    }
  }

  private SessionInfo createSessionInfo() {
    SessionInfo info = new SessionInfo();
    info.setConnectionId(this.connection.getConnectionId());
    info.setSessionId(this.getId());
    return info;
  }

  public SMQSession(int sessionMode) throws JMSException{
    this(false, 1);
  }

  public SMQSession() throws JMSException{
    this(false, 1);
  }

  @Override
  public BytesMessage createBytesMessage() throws JMSException {
    SMQByteMessage smqByteMessage = new SMQByteMessage();
    return smqByteMessage;
  }

  @Override
  public MapMessage createMapMessage() throws JMSException {
    return null;
  }

  @Override
  public Message createMessage() throws JMSException {
    return null;
  }

  @Override
  public ObjectMessage createObjectMessage() throws JMSException {
    return null;
  }

  @Override
  public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
    return null;
  }

  @Override
  public StreamMessage createStreamMessage() throws JMSException {
    return null;
  }

  @Override
  public TextMessage createTextMessage() throws JMSException {
    return new SMQTextMessage();
  }

  @Override
  public TextMessage createTextMessage(String text) throws JMSException {
    SMQTextMessage textMessage = new SMQTextMessage();
    textMessage.setText(text);
    return textMessage;
  }

  @Override
  public boolean getTransacted() throws JMSException {
    return this.isTransacted;
  }

  @Override
  public int getAcknowledgeMode() throws JMSException {
    return this.acknowledgeMode;
  }

  @Override
  public void commit() throws JMSException {
    // commit all send messages
    CommitTransaction commitTransaction = new CommitTransaction();
    commitTransaction.setTransactionId(currentTrId);
    commitTransaction.setSessionId(this.getId());
    // commit all consumed messages
    this.connection.sendSync(commitTransaction);
    currentTrId = null;
  }

  @Override
  public void rollback() throws JMSException {
    RollbackTransaction rollbackTransaction = new RollbackTransaction();
    rollbackTransaction.setTransactionId(currentTrId);
    rollbackTransaction.setSessionId(this.getId());
    this.connection.sendSync(rollbackTransaction);
    currentTrId = null;
  }

  @Override
  public void close() throws JMSException {

  }

  @Override
  public void recover() throws JMSException {

  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    return null;
  }

  @Override
  public void setMessageListener(MessageListener listener) throws JMSException {

  }

  @Override
  public void run() {

  }

  @Override
  public MessageProducer createProducer(Destination destination) throws JMSException {
    SMQProducer producer = new SMQProducer(this);
    producer.setDestination(destination);
    long prodducerId = producerIdStore.incrementAndGet();
    producer.setId(prodducerId);
    ProducerInfo producerInfo = new ProducerInfo();
    producerInfo.setId(prodducerId);
    producerInfo.setSessionId(this.getId());
    producerInfo.setConnectionId(this.getConnection().getConnectionId());
    this.connection.sendSync(producerInfo);
    producerMap.put(prodducerId, producer);
    return producer;
  }

  @Override
  public MessageConsumer createConsumer(Destination destination) throws JMSException {
    SMQOldConsumer consumer = new SMQOldConsumer();
    consumer.setQueue((QueueInfo) destination);
    consumer.setSession(this);
    consumer.setId(consumerIdStore.incrementAndGet());
    consumerMap.put(consumer.getId(), consumer);
    return consumer;
  }

  @Override
  public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
    return null;
  }

  @Override
  public Queue createQueue(String queueName) throws JMSException {
    QueueInfo info = new QueueInfo();
    info.setQueueName(queueName);
    info.setId(queueId.incrementAndGet());
    this.connection.sendSync(info);
    return info;
  }

  @Override
  public Topic createTopic(String topicName) throws JMSException {
    return null;
  }

  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
    return null;
  }

  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
    return null;
  }

  @Override
  public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
    return null;
  }

  @Override
  public QueueBrowser createBrowser(Queue queue) throws JMSException {
    SQQueueBrowser queueBrowser = new SQQueueBrowser();
    queueBrowser.setId(consumerIdStore.incrementAndGet());
    queueBrowser.setQueue(queue);
    queueBrowser.setSession(this);
    queueBrowser.start();
    consumerMap.putIfAbsent(queueBrowser.getId(), queueBrowser);
    return queueBrowser;
  }

  @Override
  public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
    return null;
  }

  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    return null;
  }

  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {
    return null;
  }

  @Override
  public void unsubscribe(String name) throws JMSException {

  }

  public Long getId(){
    return this.id;
  }

  public void setId(Long id){
    this.id = id;
  }

  private void beginTransaction() throws JMSException {
    StartTransaction startTransaction = new StartTransaction();
    currentTrId = transactioStore.incrementAndGet();
    startTransaction.setTransactionId(currentTrId);
    startTransaction.setSessionId(this.getId());
    this.connection.sendAsync(startTransaction);
  }

  public void sendMessage(SMQMessage message) throws JMSException {
    if(isTransacted && currentTrId == null){
      beginTransaction();
    }
    message.setSessionId(this.getId());
    if(isTransacted){
     // message.setResponseRequire(true);
      this.connection.sendAsync(message);
    } else {
      this.connection.sendSync(message, timeout);
    }
  }

  public void sendAsynchronously(SMQMessage message, CompletionListener listener) throws JMSException {
    message.setSessionId(this.getId());
    this.connection.sendAsync(message);
  }

  private void sendAsynchronously(Serialization serialization) throws JMSException {
    this.connection.sendAsync(serialization);
  }

  public SMQConnection getConnection() {
    return connection;
  }

  public void setConnection(SMQConnection connection) {
    this.connection = connection;
  }

  public SMQProducer getProducer(long producerId) {
    return producerMap.get(producerId);
  }

  public AutoCloseable getConsumer(long consumerId){
    return consumerMap.get(consumerId);
  }

  public void start() {

  }

  public void stop() {

  }
}
