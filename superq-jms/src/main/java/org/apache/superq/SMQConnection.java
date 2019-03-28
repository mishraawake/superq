package org.apache.superq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.superq.log.Logger;
import org.apache.superq.log.LoggerFactory;

public class SMQConnection implements javax.jms.Connection {

  Logger logger = LoggerFactory.getLogger(SMQConnection.class);

  private long connectionId;
  private String clientId;
  ConnectionMetaData connectionMetaData;
  ExceptionListener exceptionListener;
  AtomicInteger threadCount = new AtomicInteger(0);
  AtomicLong sessionIdStore = new AtomicLong(0);
  AtomicLong correlatedIdStore = new AtomicLong(0);
  WiredObjectFactory partialRequestFactory = new ConstArrayWiredObjectFactory();
  volatile SocketChannel socketChannel;
  Thread receiver;
  private AtomicBoolean started = new AtomicBoolean(false);
  Set<Long> messagesAckAwaited = new ConcurrentSkipListSet<>();
  Map<Long, Long> debugTiming = new ConcurrentHashMap<>();
  Set<Long> correlatedsAckAwaited = new ConcurrentSkipListSet<>();
  Object sendAwait = new Object();
  AtomicBoolean connectWait = new AtomicBoolean(true);
  AtomicBoolean writeWait = new AtomicBoolean(true);
  AtomicBoolean readWait = new AtomicBoolean(true);
  Map<Long, SMQSession> sessionMap = new ConcurrentHashMap<>();
  private final int defaultTimeout = 5000;
  Selector selector;
  Selector connectionSelector;
  BlockingQueue<Serialization> serializationsQueue = new LinkedBlockingQueue<>();
  SelectionKey key;
  volatile Serialization jumboText;
  volatile ByteBuffer writeBuffer;
  int remaining = Integer.BYTES + Short.BYTES;
  volatile boolean readyForPackaging = true;
  RequestReader requestReader = new RequestReader();
  ExecutorService executorService = null;
  AtomicInteger sentItem = new AtomicInteger(0);
  volatile boolean connected = false;
  volatile boolean errorConnected = false;
  WiredObjectFactory rf = new ConstArrayWiredObjectFactory();

  public SMQConnection(SocketChannel socketChannel, long connectionId, Selector selector, Selector connectionSelector) throws JMSException, IOException {
    long stime = System.currentTimeMillis();
    this.socketChannel = socketChannel;
    this.connectionId = connectionId;
    this.selector = selector;
    this.connectionSelector = connectionSelector;
  }

  public void registerForConnect() throws IOException  {
    this.connectionSelector.wakeup();
    waitToNotifyForConnect();
  }

  private void waitToNotifyForConnect() throws IOException {
    waitOn(connectWait);
    connect(this.socketChannel);
  }

  public void notifyToConnect(SocketChannel sc) {
    this.socketChannel = sc;
    wakeForObject(connectWait);
  }

  public void connect(SocketChannel socketChannel) throws IOException {
    try {
      //socketChannel.configureBlocking(true);
      boolean finishConnect =  socketChannel.finishConnect();
      if(!finishConnect) {
        System.out.println("Not Successfully connected " + this.socketChannel.socket());
        System.exit(1);
      } else {
        System.out.println("Successfully connected " + this.socketChannel.socket() + " "+socketChannel.isConnected() +" "+socketChannel.isConnectionPending() + " "+System.currentTimeMillis()  +" "+socketChannel.isOpen());
        //sleep(1000);
      }
      selector.wakeup();
      key = this.socketChannel.register(selector, SelectionKey.OP_READ);
      key.attach(this);
      this.errorConnected = false;
    } catch (Exception e) {
      this.errorConnected = true;
      throw e;
    } finally {
      this.connected = true;
      synchronized (started){
        started.notify();
      }
    }
   // this.socketChannel = socketChannel;
  }

  private ConnectionInfo sendConnectionInfo() throws JMSException {

    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.setConnectionId(getConnectionId());
    sendSync(connectionInfo);
    return connectionInfo;
  }

  public void sendSync(SMQMessage message, int timeout) throws JMSException {
    try {
      writeSerialization(message);
      messagesAckAwaited.add(message.getJmsMessageLongId());
      waitForNotify(message.getJmsMessageLongId(), timeout);
    } catch (IOException e){
      handleIOException(e);
    }
  }

  private void waitForNotify(Long messageId, int timeout) throws JMSException {
    synchronized (sendAwait){
      while (messagesAckAwaited.contains(messageId)) {
        try {
          sendAwait.wait(timeout);
        }
        catch (InterruptedException e) {
        }
      }
    }
  }

  public ExecutorService getSessionExecutor(long sessionID){
    if(executorService == null){
      executorService = Executors.newFixedThreadPool(1000, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          return new Thread( r,"Session Consumer Thread, SessionId "+sessionID + "  "+threadCount.incrementAndGet());
        }
      });

      ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)executorService;
      threadPoolExecutor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
          ////System.out.println("accessive message are being consumed, rejecting");
        }
      });
    }
    return executorService;
  }

  public void sendSync(CorrelatedPacket packet, int timeout) throws JMSException {
    try {
      packet.setPacketId(correlatedIdStore.incrementAndGet());
      packet.setDebugTime(System.currentTimeMillis());
      correlatedsAckAwaited.add(packet.getPacketId());
      writeSerialization(packet);
      waitToNotifyForCorrelated(packet, timeout);
    } catch (IOException e){
      handleIOException(e);
    }
  }

  private void waitToNotifyForCorrelated(CorrelatedPacket packet, int timeout) throws IOException {
    synchronized (key){
      key.interestOps( key.interestOps() | SelectionKey.OP_READ);
      key.selector().wakeup();
    }
    waitOn(readWait);
    readRequest(socketChannel);
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
  public
  void start() throws JMSException {
    if(started.compareAndSet(false, true)){
      while(!connected){
        synchronized (started){
          try {
            started.wait(10);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      if(errorConnected){
        throw new JMSException("Error happened in connection....");
      }
      long stime = System.currentTimeMillis();
      System.out.println(" Started writing "+socketChannel.isConnectionPending());
      ConnectionInfo connectionInfo = sendConnectionInfo();
      if(System.currentTimeMillis() - stime >= 10)
        System.out.println("connection sending time"+(System.currentTimeMillis() - stime) + " packetid "+connectionInfo.getPacketId());
      for(Map.Entry<Long, SMQSession> sessionEntry : sessionMap.entrySet()){
        sessionEntry.getValue().start();
      }
    }
  }

  @Override
  public void stop() throws JMSException {
    if(started.compareAndSet(true, false)){
      for(Map.Entry<Long, SMQSession> sessionEntry : sessionMap.entrySet()){
        sessionEntry.getValue().stop();
      }
    }
  }

  @Override
  public void close() throws JMSException {
    try {
      System.out.println("Closing...."+socketChannel.socket() + " "+Thread.currentThread().getName());
      selector.wakeup();
     // selector.close();
      socketChannel.close();
      System.out.println("Closing...."+key);
    }
    catch (IOException e) {
      e.printStackTrace();
     // handleIOException(e);
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
      writeSerialization(message);
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
   // ////System.out.println("sending messages in "+(System.currentTimeMillis() - stime));
    JumboText text = new JumboText();
    text.setBytes(bao.toByteArray());
    writeSerialization(text);
   // ////System.out.println("sending messages in "+(System.currentTimeMillis() - stime));
  }

  public void sendAsync(Serialization packet) throws JMSException {
    try {
      writeSerialization(packet);
    } catch (IOException e){
      handleIOException(e);
    }
  }

  private void handleIOException(IOException e) throws JMSException{
    JMSException jmse = new JMSException("Exception in send."+key+" "+socketChannel.socket());
    jmse.setLinkedException(e);
    //close();
    throw jmse;
  }

  private SMQSession getSession(long sessionId){
    return sessionMap.getOrDefault(sessionId, null);
  }

  private void writeSerialization(Serialization packet) throws IOException{
    serializationsQueue.add(packet);

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


  public void writeJumbo(SocketChannel sc) throws IOException {
   // ////System.out.println("Attempting to write "+readyForPackaging);
    long stime = System.currentTimeMillis();
    long startLog = 0;

    if(writeBuffer == null && jumboText != null){

      if(jumboText instanceof CorrelatedPacket){
        CorrelatedPacket correlatedPacket = (CorrelatedPacket) jumboText;
        debugTiming.putIfAbsent(((CorrelatedPacket) jumboText).getPacketId(), System.currentTimeMillis());
        log(correlatedPacket,0, "remain "+remaining+" "+readyForPackaging+" "+Thread.currentThread().getName());
        startLog = System.currentTimeMillis();
      }

      ////System.out.println("sending jumbo in ");
      remaining = jumboText.getBuffer().length + Integer.BYTES + Short.BYTES;
      writeBuffer = ByteBuffer.allocate(remaining);
      writeBuffer.putInt(jumboText.getBuffer().length);
      writeBuffer.putShort(jumboText.getType());
      writeBuffer.put(jumboText.getBuffer());
      writeBuffer.flip();
      stime = System.currentTimeMillis();
      //synchronized (SMQConnection.class) {
      if(!sc.isConnected())
         System.out.println("Just before write ");
        int write = sc.write(writeBuffer);
        if((System.currentTimeMillis() - stime) > 10){
           System.out.println("writing 1 ====="+ (System.currentTimeMillis() - stime) + "   "+write + " remaining "+remaining + "   "+System.currentTimeMillis() + " "+Thread.currentThread().getName());
        }

        remaining -= write;
      if(jumboText instanceof TextMessage){
       // System.out.println("Sending CommitTransaction Info");
        //sleep(50);
      }
      if(jumboText instanceof CorrelatedPacket){
        CorrelatedPacket correlatedPacket = (CorrelatedPacket) jumboText;
        debugTiming.putIfAbsent(((CorrelatedPacket) jumboText).getPacketId(), System.currentTimeMillis());
        log(correlatedPacket,2, "remain "+remaining+" "+readyForPackaging+" "+Thread.currentThread().getName());
        startLog = System.currentTimeMillis();
      }

      //}
    } else if(remaining > 0){
      System.out.println("Remaining...");
      int write = sc.write(writeBuffer);
      remaining -= write;
      if((System.currentTimeMillis() - stime) > 1){
        ////System.out.println("writing 2 ====="+ (System.currentTimeMillis() - stime));
      }
    }

    if(remaining == 0 && !readyForPackaging){
     //
      // System.out.println("Comple senging "+System.currentTimeMillis());
     // //System.out.println("Attempting to write remaining");
      if(jumboText instanceof CorrelatedPacket) {
        CorrelatedPacket correlatedPacket = (CorrelatedPacket) jumboText;
        //if(System.currentTimeMillis() - startLog > 0) {
          log(correlatedPacket, 3, "" + (System.currentTimeMillis() ));
        //}
        //  correlatedPacket.setDebugTime(System.currentTimeMillis());
      }
      synchronized (this) {
        writeBuffer = null;
        jumboText = null;
        readyForPackaging = true;
        //writeResponse(sc);
      }
    }

    if((System.currentTimeMillis() - stime) > 1){
      //System.out.println("writing 4 ====="+ (System.currentTimeMillis() - stime));
    }
  }

  private void log(CorrelatedPacket correlatedPacket, int space, String... str) {
    if( correlatedPacket.getDebugTime() > 0 && System.currentTimeMillis() - correlatedPacket.getDebugTime() >= 0){
     // System.out.println("start the log "+System.currentTimeMillis());
      // System.out.println(correlatedPacket.getDebugTime()+" Debug time receive"+space+" = "+(System.currentTimeMillis() - correlatedPacket.getDebugTime() ) +
         //                       " type = "+correlatedPacket.getType() + Arrays.toString(str));
        //correlatedPacket.setDebugTime(System.currentTimeMillis());
    }
  }

  private void writeResponse(SocketChannel sc) throws IOException {
    if(!readyForPackaging){
      return;
    }
    if(!serializationsQueue.isEmpty()){
      readyForPackaging = false;
    } else {
      return;
    }
    List<Serialization> serializationList = new ArrayList<>();
    while (!serializationsQueue.isEmpty()) {
      serializationList.add(serializationsQueue.poll());
    }
    if(serializationList.size() > 0) {
      createPacketAndWrite(serializationList);
    }
  }

  private void createPacketAndWrite(List<Serialization> serializationList) throws IOException {

    //System.out.println("sending jumbo in "+serializationList);
    if(serializationList.size() > 1){

      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bao);
      for(Serialization serialization: serializationList){

        dos.writeInt(serialization.getSize());
        dos.writeShort(serialization.getType());
        dos.write(serialization.getBuffer());
      }
      jumboText = new JumboText();
      ((JumboText)jumboText).setBytes(bao.toByteArray());
      ((JumboText)jumboText).setNumberOfItems(serializationList.size());
      sentItem.addAndGet(((JumboText)jumboText).getNumberOfItems());
    } else {
      jumboText = serializationList.get(0);
      if(jumboText instanceof CorrelatedPacket)
        log((CorrelatedPacket) jumboText,1, "remain "+remaining+" "+readyForPackaging+" "+Thread.currentThread().getName());
        //((CorrelatedPacket)jumboText).setDebugTime(System.currentTimeMillis());
    }

    if(key != null) {
      if (key.isValid()) {
        while (remaining > 0 ){
         // System.out.println("sending jumbo in ........"+remaining+" "+Thread.currentThread().getName());
          synchronized (key) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
          }
          key.selector().wakeup();
          waitOn(writeWait);
          writeJumbo(socketChannel);

         // System.out.println("sending jumbo in ........"+remaining+" "+Thread.currentThread().getName());
        }
        remaining = Integer.BYTES + Short.BYTES;
      } else {
        //System.out.println("Invalid key..");
      }
    }
  }

  private void waitOn(AtomicBoolean waitObject) {
    try {
      synchronized (waitObject) {
        while(waitObject.get()){
          waitObject.wait();
        }
        waitObject.set(true);
      }
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void wakeForObject(AtomicBoolean obj){
    synchronized (obj) {
      obj.set(false);
      obj.notify();
    }
  }

  public void wakeForWrite(){
    wakeForObject(writeWait);
  }

  public void wakeForRead(){
    wakeForObject(readWait);
  }

  public void readRequest(SocketChannel sc) throws IOException {
      requestReader.tryComplete(sc);
      if(requestReader.complete()){
        handleIncomingPacket(requestReader.getObject());
      } else {
        synchronized (key) {
          key.interestOps(key.interestOps() | SelectionKey.OP_READ);
          key.selector().wakeup();
        }
        waitOn(readWait);
        readRequest(sc);
      }
  }

  private void handleIncomingPacket(Serialization packet) throws IOException {
    ////System.out.println("packet");
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
        Serialization textMessage = null;
        try {
          textMessage = (Serialization) partialRequestFactory.getInitialPartialRequest(type).newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
          throw new IOException(e);
        }

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
      ////System.out.println("packet");
      CorrelatedPacket correlatedPacket = (CorrelatedPacket)packet;
      correlatedPacket.getPacketId();
     // //System.out.println(correlatedPacket.getDebugTime());
      log(correlatedPacket, 4);
      synchronized (sendAwait) {
        SMQConnection.this.correlatedsAckAwaited.remove(correlatedPacket.getPacketId());
        if(debugTiming.containsKey(correlatedPacket.getPacketId())) {
          long time = System.currentTimeMillis() - debugTiming.get(correlatedPacket.getPacketId());
          if (time > 100)
            System.out.println("timing in packet " + (time) + " packet " + correlatedPacket.getPacketId() + " type " + correlatedPacket.getType());
        }
        sendAwait.notifyAll();
      }
    }
  }

  private void handleConsumerMessage(SMQMessage packet) {
    SMQMessage message = packet;
    SMQSession session = SMQConnection.this.getSession(message.getSessionId());
    AutoCloseable mc = session.getConsumer(message.getConsumerId());
    session.deliver(message);
  }

  class RequestReader {

    protected ByteBuffer bb;
    protected boolean startedReading = false;
    private final int INT_PLUS_MESSAGE_TYE = 4 + 2;
    protected int remaining = INT_PLUS_MESSAGE_TYE + 1;
    ByteBuffer size = ByteBuffer.allocate(INT_PLUS_MESSAGE_TYE);
    private boolean emptyByte = true;
    short messageType;

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
        return;
      }
      if(remaining > 0){
        remaining -= readOrThrowException(ssc, bb);
      } else {
        bb.flip();
      }
    }

    protected int readOrThrowException(SocketChannel ssc, ByteBuffer bb) throws IOException {
      long stime = System.currentTimeMillis();
      int readBytes = ssc.read(bb);
      if(System.currentTimeMillis() - stime > 0)
      System.out.println("Individual Reading time ====="+(System.currentTimeMillis() - stime));
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
