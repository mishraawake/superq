package org.apache.superq.storage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.JMSException;

import org.apache.superq.Broker;
import org.apache.superq.ConsumerInfo;
import org.apache.superq.ProduceAck;
import org.apache.superq.QueueInfo;
import org.apache.superq.SMQMessage;
import org.apache.superq.Task;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.log.Logger;
import org.apache.superq.log.LoggerFactory;
import org.apache.superq.outgoing.SBConsumer;

public class SBQueueDefault implements SBQueue<SMQMessage> {

  Logger logger = LoggerFactory.getLogger(SBQueueDefault.class);

  MessageSupplier messageSupplier;

  MessageStore store;

  Broker broker;


  TransactionSync afterTransactiondispatch = new TransactionSync() {
    @Override
    public void afterCommit() {
      try {
        dispatchProcess();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  };

  public SBQueueDefault(Broker broker, QueueInfo info, TransactionalStore transactionalStore) throws IOException, JMSException {
    this.broker = broker;
    store = new MessageStoreImpl(info.getQueueName());
    messageSupplier = new RamMessageSupplier(store, info.getQueueName());
  }

  ConcurrentMap<String, ConsumerInfo> consumerMap = new ConcurrentHashMap<>();

  @Override
  public void acceptMessage(SMQMessage message, SBProducerContext producerContext) throws IOException {
    if(isResourceNotAvailable()){

      // handle scenarios when running under resource
    }

    if(producerContext.getSessionContext().isTransactional()){
      TransactionalStore transactionalStore = producerContext.getSessionContext().getCurrentTransaction();
      if(message.isPersistent()) {
        transactionalStore.addMessage(message);
      } else {
        transactionalStore.addSynchrounization(new TransactionSync() {
          @Override
          public void afterCommit() {
            try {
              messageSupplier.addMessage(message);
            }
            catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      }
      transactionalStore.addSynchrounization(afterTransactiondispatch);
      // we do not want to trigger dispatch process for message in transaction.
    } else {
      if (message.isPersistent()) {
        broker.enqueueFileIo(new Task() {
          @Override
          public void perform() throws Exception {
            store.addMessage(message);
          }
        }, new Task() {
          @Override
          public void perform() throws Exception {
            dispatchProcess();
          }
        });
      } else {
        messageSupplier.addMessage(message);
        dispatchProcess();
      }

    }

    if(message.isResponseRequire()){
      producerContext.getConnectionContext().sendAsyncPacket(new ProduceAck());
    }

  }

  private boolean isResourceNotAvailable() {
    return false;
  }

  @Override
  public void acceptConsumer(SBConsumer<SMQMessage> consumer) {
    // add to the list of consumers
    // it will only do anything if it has been started successfully.
    // It will add the consumers, prepare it and then start a process of dispatching the
    // message
    // the dispatch method now has to notify that a new consumer is prepared to take up the message.

  }

  @Override
  public List<SBConsumer<SMQMessage>> getConsumers() {
    // return the list of  consumers
    return null;
  }

  @Override
  public void start() {
    // it may be started by initial server.
    // to understand which queue will be started and how it will be started.

    // it may be started by a command of creating the queue


    // it may be created by a incoming consumer, earlier this queue does not have any consumers

    // to start it may provision some resources for itself. This also depends on several thing depends
    // upon a saved data about the history of the queue or its configurations

    // TO start with, it will not have any consumers attached, should it warm some messages

  }

  @Override
  public void prepare() {
    // prepare to be started. Provision resources etc.
  }

  @Override
  public SMQMessage pullMessage() {
    // if this method is called, queue will fetch message and handover to this method. The difference in
    // pull and dispatch is that dispatch handover the dispatched message to consumer but this method will
    // get the message and handover to this message. This method will get message in same underlying resource
    // from where dispatch get. After handing it over it will put this message in ackAwaited list.
    return null;
  }

  @Override
  public void ackMessage(SMQMessage message) {
    // ack message in doing so delete the message from the queue knowledge and also notify this event
    // the dispatching process would be interested into this event because it might have been stalled
    // because of limit of unack messages.
  }

  @Override
  public boolean canAcceptMoreConsumer() {
    return false;
  }

  @Override
  public QStatus getStatus() {
    return null;
  }

  @Override
  public void addConstraint() {

  }

  @Override
  public List<QConstraint> getConstraints() {
    return null;
  }

  @Override
  public void dispose() {
    // dispose consumers one my one. Wait for unack message to some defined threshold and then clear the message queue.
  }

  @Override
  public void onConsumerReadyForMessage(SBConsumer<SMQMessage> messageSBConsumer) {

  }

  @Override
  public void putMessageOnAnotherConsumerGroup(SBConsumer<SMQMessage> messageSBConsumer) {

  }

  // when a fresh set of consumers trying to connect to this queue, this queue will wait a configurable
  // amount of time to save the first consumer getting prefetch message.
  private void consumersReadyWait(){

  }

  private void dispatchProcess() throws IOException {
    // get the message one by one
    // hand it over to consumer in round robin fashion or based on group id hand it over to only one consumer.
    // put all handover message to the ack list.

    while(messageSupplier.hasMoreMessage()) {
      SMQMessage message = getMessage();
      SBConsumer<SMQMessage> consumer = getNextConsumer(), fistConsumer = consumer;
      boolean matches = true;
      while (!(consumer.canAcceptMoreMessage() && consumer.matches(message) &&
              doesGroupApply(message.getGroupId(), message.getGroupSeqId(), message, consumer))) {
        consumer = getNextConsumer();
        if (consumer == fistConsumer) {
          matches = false;
          break;
        }
      }
      if (!matches) {
        logger.errorLog("no matcher found for message {} ", message);
        // this message will be reattempted again.
        break;
      }
      else {
        consumer.dispatch(message);
      }
    }
  }

  /**
   * return true if there is one consumer available to take this message, otherwise no.
   * @param groupId
   * @param message
   * @return
   */
  private boolean doesGroupApply(String groupId, int sequence, SMQMessage message, SBConsumer<SMQMessage> consumer){
    if(groupId != null){
      if(sequence == 1) {
        // assign the group
        consumer.assignGroup(groupId);
        consumerMap.put(groupId, consumer.getConsumerInfo());
      } else {
        if(consumerMap.containsKey(groupId) ) {
          ConsumerInfo info = consumerMap.get(groupId);
          if(isValidConsumer(info)){
            if(!info.equals(consumer.getConsumerInfo())){
              return false;
            } else {
              if (sequence < 0) {
                consumerMap.remove(groupId);
              }
            }
          } else {
            consumer.assignGroup(groupId);
            consumerMap.put(groupId, consumer.getConsumerInfo());
            // this means this we need to shift this group to some other consumer.
          }
        }
      }
    }
    return true;
  }

  // while consuming the message, consumer will have separate storage, primarily ram and storage. This
  // method will hint consumer to start a process to fetch the next batch of messages.
  private boolean shouldFetchNextMessageBatch(int size){
    return false;
  }

  private void onMessageLeftInMemory(){

  }

  // this is called when message from the message source will be ready to be sent to the consumers.
  // the source of this method could be message storage.
  private void onMessageFetchReady(){

  }

  // this actually relays the message to the consumer
  private void relayMessages(){
    // get message one by one. and dispatch it according to the weightage algorithm
  }

  private boolean isValidConsumer(ConsumerInfo info){
    return false;
  }

  //it returns the next message to be delivered
  private SMQMessage getMessage() throws IOException {
    if(messageSupplier.hasMoreMessage()){
      return messageSupplier.getNextMessage();
    }
    // this means that queue has no more message, so it has to wait for more message.
    return null;
  }

  // it gives next ready consumer for accepting the message.
  private SBConsumer<SMQMessage> getNextConsumer(){
    return null;
  }
}
