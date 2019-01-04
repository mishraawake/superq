package org.apache.superq;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.superq.log.Logger;
import org.apache.superq.log.LoggerFactory;

public class SMQConnection implements javax.jms.Connection {

  Logger logger = LoggerFactory.getLogger(SMQConnection.class);

  private long connectionId;
  private String clientId;
  ConnectionMetaData connectionMetaData;
  ExceptionListener exceptionListener;
  AtomicLong sessionIdStore = new AtomicLong(0);
  AtomicLong correlatedIdStore = new AtomicLong(0);
  WiredObjectFactory partialRequestFactory = new ConstArrayWiredObjectFactory();
  Socket socket;
  Thread receiver;
  private AtomicBoolean started = new AtomicBoolean(false);
  Set<Long> messagesAckAwaited = new ConcurrentSkipListSet<>();
  Set<Long> correlatedsAckAwaited = new ConcurrentSkipListSet<>();
  Object sendAwait = new Object();
  Map<Long, SMQSession> sessionMap = new ConcurrentHashMap<>();
  private final int defaultTimeout = 5000;

  public SMQConnection(Socket socket, long connectionId) throws JMSException {
    this.socket = socket;
    this.connectionId = connectionId;
    Thread thread = new Thread(new Receiver());
    thread.start();
    sendConnectionInfo();
  }

  private void sendConnectionInfo() throws JMSException {
    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.setConnectionId(getConnectionId());
    connectionInfo.setPacketId(correlatedIdStore.incrementAndGet());
    sendSync(connectionInfo);
  }

  public void sendSync(SMQMessage message, int timeout) throws JMSException {
    try {
      writeSerialization(message);
      messagesAckAwaited.add(Long.valueOf(message.getJMSMessageID()));
      long stime = System.currentTimeMillis();
      while (messagesAckAwaited.contains(Long.valueOf(message.getJMSMessageID()))) {
        try {
          synchronized (sendAwait) {
            sendAwait.wait(timeout);
          }
        }
        catch (InterruptedException e) {
        }
      }
    } catch (IOException e){
      handleIOException(e);
    }
  }

  public void sendSync(CorrelatedPacket packet, int timeout) throws JMSException {
    try {
      packet.setPacketId(correlatedIdStore.incrementAndGet());
      writeSerialization(packet);
      correlatedsAckAwaited.add(Long.valueOf(packet.getPacketId()));
      long stime = System.currentTimeMillis();
      while (correlatedsAckAwaited.contains(Long.valueOf(packet.getPacketId()))) {
        try {
          synchronized (sendAwait) {
            sendAwait.wait(timeout);
          }
        }
        catch (InterruptedException e) {
        }
      }
    } catch (IOException e){
      handleIOException(e);
    }
  }

  public void sendSync(CorrelatedPacket packet) throws JMSException {
    sendSync(packet, defaultTimeout);
  }


  @Override
  public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
    SMQSession session = new SMQSession(transacted, acknowledgeMode);
    return assignIdAndStore(session);
  }

  private SMQSession assignIdAndStore(SMQSession session) throws JMSException {
    long sessionId = sessionIdStore.incrementAndGet();
    session.setId(sessionId);
    session.setConnection(this);
    sessionMap.put(sessionId, session);
    session.initialize();
    return session;
  }

  @Override
  public Session createSession(int sessionMode) throws JMSException {
    SMQSession session = new SMQSession(sessionMode);
    return assignIdAndStore(session);
  }

  @Override
  public Session createSession() throws JMSException {
    SMQSession session = new SMQSession(false, 1);
    return assignIdAndStore(session);
  }

  @Override
  public String getClientID() throws JMSException {
    return this.clientId;
  }

  @Override
  public void setClientID(String clientID) throws JMSException {
    this.clientId = clientID;
  }

  @Override
  public ConnectionMetaData getMetaData() throws JMSException {
    return this.connectionMetaData;
  }

  @Override
  public ExceptionListener getExceptionListener() throws JMSException {
    return this.exceptionListener;
  }

  @Override
  public void setExceptionListener(ExceptionListener listener) throws JMSException {
    this.exceptionListener = listener;
  }

  @Override
  public void start() throws JMSException {
    if(!started.compareAndSet(false, true)){
      for(Map.Entry<Long, SMQSession> sessionEntry : sessionMap.entrySet()){
        sessionEntry.getValue().start();
      }
    }
  }

  @Override
  public void stop() throws JMSException {
    if(!started.compareAndSet(true, false)){
      for(Map.Entry<Long, SMQSession> sessionEntry : sessionMap.entrySet()){
        sessionEntry.getValue().stop();
      }
    }
  }

  @Override
  public void close() throws JMSException {
    try {
      socket.close();
    }
    catch (IOException e) {
      handleIOException(e);
    }
  }

  @Override
  public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    return null;
  }

  @Override
  public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    return null;
  }

  @Override
  public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    return null;
  }

  @Override
  public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    return null;
  }

  public void sendAsync(SMQMessage message) throws JMSException {
    try {
      writeSerialization(message);
    } catch (IOException e){
      handleIOException(e);
    }
  }

  public void sendAsync(Serialization packet) throws JMSException {
    try {
      writeSerialization(packet);
    } catch (IOException e){
      handleIOException(e);
    }
  }

  private void handleIOException(IOException e) throws JMSException{
    JMSException jmse = new JMSException("Exception in send.");
    jmse.setLinkedException(e);
    close();
    throw jmse;
  }

  private SMQSession getSession(long sessionId){
    return sessionMap.getOrDefault(sessionId, null);
  }

  private void writeSerialization(Serialization packet) throws IOException{
    ByteBuffer bb = ByteBuffer.allocate(6);
    bb.putInt(packet.getSize());
    bb.putShort(packet.getType());
    bb.flip();
    socket.getOutputStream ().write(bb.array());
    socket.getOutputStream ().write(packet.getBuffer());
  }

  public boolean isStarted() {
    return started.get();
  }

  class Receiver implements Runnable {

    @Override
    public void run() {
      System.out.println("Receiver thread started");
      while (true){
        Object packet = null;
        try {
           packet = getNextPacket(socket);
        } catch (IOException ioe){
          try {
            handleIOException(ioe);
          }
          catch (JMSException e) {
            logger.errorLog("Connection is closing ", e);
            e.printStackTrace();
          }
          break;
        }
        if(packet instanceof ProduceAck){
          ProduceAck pack = (ProduceAck)packet;
          SMQProducer producer = SMQConnection.this.getSession(pack.getSessionId()).getProducer(pack.getProducerId());
          AsyncProduceData asyncProduceData = producer.getMessageHandler(pack.getMessageId());
          if(asyncProduceData == null){
            producer.decrementInFlightMessage();
            SMQConnection.this.messagesAckAwaited.remove(pack.getMessageId());
            synchronized (sendAwait) {
              sendAwait.notifyAll();
            }
          } else{
            asyncProduceData.getCompletionListener().onCompletion(asyncProduceData.getMessage());
          }
        } else if(packet instanceof SMQMessage){
          handleConsumerMessage((SMQMessage) packet);
        } else if(packet instanceof CorrelatedPacket){
          CorrelatedPacket correlatedPacket = (CorrelatedPacket)packet;
          correlatedPacket.getPacketId();
          SMQConnection.this.correlatedsAckAwaited.remove(correlatedPacket.getPacketId());
          synchronized (sendAwait) {
            sendAwait.notifyAll();
          }
        }
      }
    }
  }

  private void handleConsumerMessage(SMQMessage packet) {
    SMQMessage message = packet;
    SMQSession session = SMQConnection.this.getSession(message.getSessionId());
    AutoCloseable mc = session.getConsumer(message.getConsumerId());
    if(mc instanceof MessageConsumer){
      SMQOldConsumer consumer = (SMQOldConsumer)mc;
      try {
        consumer.getMessageListener().onMessage(message);
      } catch (JMSException e){

      }
      try {
        if(session.getTransacted()){
          // do nothing
        }else if (session.getAcknowledgeMode() == Session.AUTO_ACKNOWLEDGE || session.getAcknowledgeMode() == Session.DUPS_OK_ACKNOWLEDGE) {
          ConsumerAck consumerAck = new ConsumerAck();
          consumerAck.setMessageId(message.getJmsMessageLongId());
          consumerAck.setConnectionId(this.getConnectionId());
          consumerAck.setSessionId(session.getId());
          consumerAck.setQid(consumer.getQueue().getId());
          consumerAck.setId(message.getConsumerId());
          sendAsync(consumerAck);
        }
      } catch (JMSException e){
      }
    } else {
      SQQueueBrowser consumer = (SQQueueBrowser)mc;
      consumer.consumer(packet);
    }
  }

  private Serialization getNextPacket(Socket socket) throws IOException {
    DataInputStream dis = new DataInputStream(socket.getInputStream());
    return partialRequestFactory.getObject(dis);
  }

  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(long connectionId) {
    this.connectionId = connectionId;
  }
}
