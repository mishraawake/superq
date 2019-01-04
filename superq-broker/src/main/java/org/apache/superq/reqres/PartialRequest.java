package org.apache.superq.reqres;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.superq.Broker;
import org.apache.superq.ConstArrayWiredObjectFactory;
import org.apache.superq.WiredObjectFactory;
import org.apache.superq.Serialization;
import org.apache.superq.Task;
import org.apache.superq.network.BrokerServer;
import org.apache.superq.network.ConnectionContext;

public class PartialRequest implements Partial {

  public PartialRequest(){
  }

  protected BrokerServer ser;
  protected Broker broker;
  protected SocketChannel ssc;
  protected boolean startedReading = false;
  private final int INT_PLUS_MESSAGE_TYE = 4 + 2;
  protected int remaining = INT_PLUS_MESSAGE_TYE + 1;
  ByteBuffer size = ByteBuffer.allocate(INT_PLUS_MESSAGE_TYE);
  WiredObjectFactory rf = new ConstArrayWiredObjectFactory();
  private PartialRequest partialRequest = null;
  short messageType;
  static Map<String, RequestHandler> handlerObject = new ConcurrentHashMap<>();

  static {
    for(Class classToMap : WiredObjectFactory.classTypes ){
      String handlerName = "org.apache.superq.reqres." + classToMap.getSimpleName() + "Handler";
      RequestHandler classOfMessage = null;
      try {
        classOfMessage = (RequestHandler)Class.forName(handlerName).newInstance();
      }
      catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw  new RuntimeException("Handlers for "+handlerName+" is not defined");
      }
      handlerObject.put(classToMap.getName(), classOfMessage);
    }
  }

  protected ByteBuffer bb;

  public void tryComplete(SocketChannel ssc) throws IOException {

    if(!startedReading){
      readOrThrowException(ssc, size);
      startedReading = true;
      return;
    }
    if(size.position() < INT_PLUS_MESSAGE_TYE){
      readOrThrowException(ssc, size);
      return;
    } else if(bb == null) {
      size.flip();
      remaining = size.getInt();
      messageType = size.getShort();
      bb = ByteBuffer.allocate(remaining);
      remaining -= readOrThrowException(ssc, bb);
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

  public boolean complete(){
    return remaining == 0;
  }

  @Override
  public Task handle(final ConnectionContext context) throws IOException {
    return new Task(){
      @Override
      public void perform() throws Exception {
        Class classOfMessage = rf.getInitialPartialRequest(messageType);
        Serialization instantiate = (Serialization)classOfMessage.newInstance();
        instantiate.acceptByteBuffer(bb.array());
        handlerObject.get(classOfMessage.getName()).handle(instantiate, context);
      }
    };
  }

  public BrokerServer getSer() {
    return ser;
  }

  public void setSer(BrokerServer ser) {
    this.ser = ser;
  }

}
