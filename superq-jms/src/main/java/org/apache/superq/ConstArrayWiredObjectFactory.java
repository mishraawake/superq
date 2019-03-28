package org.apache.superq;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Selector;
import javax.jms.JMSException;

public class ConstArrayWiredObjectFactory implements WiredObjectFactory {

  @Override
  public Class getInitialPartialRequest(short type) {
    return classTypes[type];
  }

  @Override
  public Serialization getObject(DataInputStream dis) throws IOException {
    int size = dis.readInt();
    short type = dis.readShort();
    Class classOfMessage = classTypes[type];
    byte[] restOfTheBytes = new byte[size];
    dis.readFully(restOfTheBytes);
    Object instantiate = null;
    try {
       instantiate = classOfMessage.newInstance();
    } catch (InstantiationException | IllegalAccessException illegalAccessException){
      IOException ioe = new IOException("Exception in instantiating of type "+type, illegalAccessException);
      throw ioe;
    }
    ((Serialization)instantiate).acceptByteBuffer(restOfTheBytes);
    return (SerializationSupport) instantiate;
  }

  public static void main(String[] args) throws IOException, JMSException {
    Selector selector = Selector.open();
    long stime = System.currentTimeMillis();
    SMQConnection connection = new SMQConnection(null, 1, selector, null);
    System.out.println(System.currentTimeMillis() - stime);
  }
}
