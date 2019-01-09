package org.apache.superq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.superq.log.Logger;
import org.apache.superq.log.LoggerFactory;
import sun.jvm.hotspot.debugger.ReadResult;

public class SMQConnection implements javax.jms.Connection {

  Logger logger = LoggerFactory.getLogger(SMQConnection.class);

  private long connectionId;
  private String clientId;
  ConnectionMetaData connectionMetaData;
  ExceptionListener exceptionListener;
  AtomicLong sessionIdStore = new AtomicLong(0);
  AtomicLong correlatedIdStore = new AtomicLong(0);
  WiredObjectFactory partialRequestFactory = new ConstArrayWiredObjectFactory();
  SocketChannel socketChannel;
  Thread receiver;
  private AtomicBoolean started = new AtomicBoolean(false);
  Set<Long> messagesAckAwaited = new ConcurrentSkipListSet<>();
  Set<Long> correlatedsAckAwaited = new ConcurrentSkipListSet<>();
  Object sendAwait = new Object();
  Map<Long, SMQSession> sessionMap = new ConcurrentHashMap<>();
  private final int defaultTimeout = 5000;
  Selector selector;
  BlockingQueue<Serialization> serializationsQueue = new LinkedBlockingQueue<>();
  SelectionKey key;
  volatile JumboText jumboText;
  ByteBuffer writeBuffer;
  int remaining = 0;
  volatile boolean readyForPackaging = true;
  RequestReader requestReader = new RequestReader();

  public SMQConnection(SocketChannel socketChannel, long connectionId) throws JMSException, IOException {
    this.socketChannel = socketChannel;
    this.connectionId = connectionId;
    selector = Selector.open();
    key = socketChannel.register(selector, SelectionKey.OP_READ);
    Thread thread = new Thread(new NetworkThread());
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
      writeSerialization(message, true);
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
      writeSerialization(packet, true);
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
      socketChannel.close();
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

  private List<SMQMessage> sampledMesssages = new ArrayList<>();

  public void sendAsync(SMQMessage message) throws JMSException {
    try {
      writeSerialization(message, false);
    } catch (IOException e){
      handleIOException(e);
    }
  }

  private void writeAllMessages(List<SMQMessage> sampledMesssages) throws IOException {

    long stime = System.currentTimeMillis();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bao);
    for(SMQMessage message: sampledMesssages){
      dos.writeInt(message.getSize());
     // dos.writeShort(message.getType());
      dos.write(message.getBuffer());
    }
   // System.out.println("sending messages in "+(System.currentTimeMillis() - stime));
    JumboText text = new JumboText();
    text.setBytes(bao.toByteArray());
    writeSerialization(text, true);
   // System.out.println("sending messages in "+(System.currentTimeMillis() - stime));
  }

  public void sendAsync(Serialization packet) throws JMSException {
    try {
      writeSerialization(packet, false);
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

  private synchronized void writeSerialization(Serialization packet, boolean force) throws IOException{
    serializationsQueue.add(packet);
    if(force){
      while(!readyForPackaging){
        sleep(10);
      }
    }
    if(readyForPackaging){
      writeResponse(socketChannel);
    }
  }

  private void sleep(int n){
    try {
      Thread.sleep(n);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean isStarted() {
    return started.get();
  }

  private void infiniteSelect() throws IOException {
    while(true){
      int selectResult = selector.select();
      Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
      while (iterator.hasNext()) {
        SelectionKey next = iterator.next();
        iterator.remove();
        SelectableChannel selectableChannel = next.channel();
        if (!next.isValid()) {
          continue;
        }
        if (next.isValid() && next.isReadable()) {
          SocketChannel sc = (SocketChannel) selectableChannel;
          readRequest(sc);
        }
        if (next.isValid() && next.isWritable()) {
          SocketChannel sc = (SocketChannel) selectableChannel;
          writeJumbo(sc);
        }
      }
    }
  }

  private void writeJumbo(SocketChannel sc) throws IOException {
    if(writeBuffer == null && jumboText != null){
      remaining = jumboText.getBuffer().length + Integer.BYTES + Short.BYTES;
      writeBuffer = ByteBuffer.allocate(remaining);
      writeBuffer.putInt(jumboText.getBuffer().length);
      writeBuffer.putShort(jumboText.getType());
      writeBuffer.put(jumboText.getBuffer());
      writeBuffer.flip();
      int write = sc.write(writeBuffer);
      remaining -= write;
    } else if(remaining > 0){
      int write = sc.write(writeBuffer);
      remaining -= write;
    }

    if(remaining == 0){
      writeBuffer = null;
      jumboText = null;
      synchronized (key) {
        if (key.isValid()) {
          key.interestOps(SelectionKey.OP_READ);
          key.selector().wakeup();
        }
        readyForPackaging = true;
      }
    }
  }

  private void writeResponse(SocketChannel sc) throws IOException {

    List<Serialization> serializationList = new ArrayList<>();
    synchronized (this) {
      while (!serializationsQueue.isEmpty()) {
        serializationList.add(serializationsQueue.poll());
      }
      if(serializationList.size() > 0)
        readyForPackaging = false;
    }

    if(serializationList.size() > 0)
      createPacketAndWrite(serializationList);
  }

  private void createPacketAndWrite(List<Serialization> serializationList) throws IOException {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bao);
    for(Serialization serialization: serializationList){
      if(serialization instanceof CorrelatedPacket){
        CorrelatedPacket correlatedPacket = (CorrelatedPacket)serialization;
        correlatedPacket.setPacketId(correlatedIdStore.incrementAndGet());
      }
      if(serialization instanceof CommitTransaction){
        System.out.println("sending commit packet "+ ((CommitTransaction)serialization).getTransactionId());
      }
      dos.writeInt(serialization.getSize());
      dos.writeShort(serialization.getType());
      dos.write(serialization.getBuffer());
    }

    // System.out.println("sending messages in "+(System.currentTimeMillis() - stime));
    jumboText = new JumboText();
    jumboText.setBytes(bao.toByteArray());
    jumboText.setNumberOfItems(serializationList.size());
    if(key != null)
    synchronized (key) {
      if (key.isValid()) {
        key.interestOps( SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        key.selector().wakeup();
      }
    }
  }

  private void readRequest(SocketChannel sc) throws IOException {
      requestReader.tryComplete(sc);
      if(requestReader.complete()){
        handleIncomingPacket(requestReader.getObject());
      }
  }

  private void handleIncomingPacket(Serialization packet) throws IOException {
    if(packet instanceof JumboText) {
      JumboText jumboText = (JumboText)packet;
      ByteArrayInputStream inputStream = new ByteArrayInputStream(jumboText.getBytes());
      DataInputStream dataInputStream = new DataInputStream(inputStream);
      int count = 0;
      Serialization first = null;
      while(count < jumboText.getNumberOfItems()){
        int length = dataInputStream.readInt();
        short type = dataInputStream.readShort();
        byte[] bytes = new byte[length];
        dataInputStream.read(bytes);
        SMQTextMessage textMessage = new SMQTextMessage();
        textMessage.acceptByteBuffer(bytes);
        handleIncomingPacket(textMessage);
        ++count;
      }
    } else if(packet instanceof ProduceAck){
      ProduceAck pack = (ProduceAck)packet;
          /*SMQProducer producer = SMQConnection.this.getSession(pack.getSessionId()).getProducer(pack.getProducerId());
          AsyncProduceData asyncProduceData = producer.getMessageHandler(pack.getMessageId());
          if(asyncProduceData == null){
            producer.decrementInFlightMessage();
            SMQConnection.this.messagesAckAwaited.remove(pack.getMessageId());
            synchronized (sendAwait) {
              sendAwait.notifyAll();
            }
          } else{
            asyncProduceData.getCompletionListener().onCompletion(asyncProduceData.getMessage());
          }*/
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

  class NetworkThread implements Runnable {

    @Override
    public void run() {

      System.out.println("NetworkThread thread started");
      try {
        infiniteSelect();
      }
      catch (IOException e) {
        e.printStackTrace();
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

  class RequestReader {

    protected ByteBuffer bb;
    protected boolean startedReading = false;
    private final int INT_PLUS_MESSAGE_TYE = 4 + 2;
    protected int remaining = INT_PLUS_MESSAGE_TYE + 1;
    ByteBuffer size = ByteBuffer.allocate(INT_PLUS_MESSAGE_TYE);
    private boolean emptyByte = true;
    short messageType;
    WiredObjectFactory rf = new ConstArrayWiredObjectFactory();

    public void tryComplete(SocketChannel ssc) throws IOException {

      if(!startedReading){
        readOrThrowException(ssc, size);
        startedReading = true;
        return;
      }
      if(size.position() < INT_PLUS_MESSAGE_TYE){
        readOrThrowException(ssc, size);
        return;
      } else if(emptyByte) {
        size.flip();
        remaining = size.getInt();
        messageType = size.getShort();
        bb = ByteBuffer.allocate(remaining);
        remaining -= readOrThrowException(ssc, bb);
        emptyByte = false;
      }
      if(remaining > 0){
        remaining -= readOrThrowException(ssc, bb);
      } else {
        bb.flip();
      }
    }

    protected int readOrThrowException(SocketChannel ssc, ByteBuffer bb) throws IOException {
      int readBytes = ssc.read(bb);
      if(readBytes < 0){
        throw new IOException("Channel "+ssc);
      }
      return readBytes;
    }

    private boolean complete(){
      return remaining == 0;
    }

    protected Serialization getObject() throws IOException {
      try {
        Class classOfMessage = rf.getInitialPartialRequest(messageType);
        Serialization instantiate = (Serialization) classOfMessage.newInstance();
        instantiate.acceptByteBuffer(bb.array());
        startedReading = false;
        remaining = INT_PLUS_MESSAGE_TYE + 1;
        size = ByteBuffer.allocate(INT_PLUS_MESSAGE_TYE);
        emptyByte = true;
        return instantiate;
      }catch (Exception e){
        e.printStackTrace();
      }
      return null;
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
