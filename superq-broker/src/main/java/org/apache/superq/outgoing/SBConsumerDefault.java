package org.apache.superq.outgoing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.superq.ConsumerInfo;
import org.apache.superq.SMQMessage;
import org.apache.superq.datatype.MessageGroup;
import org.apache.superq.log.Logger;
import org.apache.superq.log.LoggerFactory;
import org.apache.superq.network.SessionContext;
import org.apache.superq.storage.Constraint;
import org.apache.superq.storage.SBQueue;

public class SBConsumerDefault implements SBConsumer<SMQMessage> {

  Logger logger = LoggerFactory.getLogger(SBConsumerDefault.class);
  ConsumerInfo info;
  SessionContext sessionContext;


  public SBConsumerDefault(ConsumerInfo info, SessionContext sessionContext){
    this.info = info;
    this.sessionContext = sessionContext;
  }

  int outstandingAck = 0;
  final int maxUnackMessages = 1000;
  // 0: starting, 1: started, 2: closing
  private int state = 0;
  private Map<Long, SMQMessage> unacks = new ConcurrentHashMap<>();
  private SBQueue<SMQMessage> queue;

  @Override
  public ConsumerInfo getConsumerInfo() {
    return info;
  }

  @Override
  public void dispatch(SMQMessage message) {
    if(outstandingAck >= maxUnackMessages){
      return;
    }
    if(state == 0){
      // its not ready yet
      return;
    }
    if(state == 2){
      logger.warnLog("Consumer is closing so can not relay the messages");
    }
    ++outstandingAck;
    unacks.putIfAbsent(message.getJmsMessageLongId(), message);
    doDispatch(message);
    // actual dispatch the message
    // state when consumer is
  }

  private void doDispatch(SMQMessage message){
    message.setSessionId(sessionContext.getSessionInfo().getSessionId());
    message.setConsumerId(info.getId());
    message.setConnectionId(sessionContext.getConnectionContext().getInfo().getConnectionId());
    sessionContext.getConnectionContext().sendAsyncPacket(message);
    logger.infoLog("dispatched message with id {} ");
  }


  @Override
  public void prepare() {

  }

  @Override
  public void start() {
    state = 1;
  }

  @Override
  public void dispose() {

  }

  @Override
  public SMQMessage pull() {
    SMQMessage message = queue.pullMessage();
    dispatch(message);
    return null;
  }


  @Override
  public void ack(long messageId) {
    unacks.remove(messageId);
    outstandingAck--;

    // for the ack of default message
    // batched ack, remove ack and remove actual message
    // individual ack
    // ack of
    // ack of poisonous message
  }

  @Override
  public int outstandingAcks() {
    return outstandingAck;
  }

  @Override
  public List<Constraint> getConstraints() {
    return null;
  }

  @Override
  public void addConstraint(Constraint constraint) {

  }

  @Override
  public ConsumerStatus getConsumerStatus() {
    return null;
  }

  @Override
  public boolean canAcceptMoreMessage() {
    return outstandingAck < maxUnackMessages;
  }

  @Override
  public MessageGroup associatedMessageGroup() {
    return null;
  }

  @Override
  public boolean matches(SMQMessage message) {
    return true;
  }

  @Override
  public Set<String> ownedGroups() {
    return null;
  }

  @Override
  public void assignGroup(String groupId) {

  }

  @Override
  public void groupEnd() {

  }

  @Override
  public void unAssignGroup() {

  }

  @Override
  public void ownGroup(String groupId) {

  }
}
