package org.apache.superq;

import java.io.DataInputStream;
import java.io.IOException;

public interface WiredObjectFactory {

  public static Class[] classTypes = new Class[]{
          SMQTextMessage.class,//0
          ConnectionInfo.class, //1
          ProducerInfo.class, //2
          StartTransaction.class, //3
          CommitTransaction.class, //4
          RollbackTransaction.class, //5
          ProduceAck.class, //6
          QueueInfo.class, //7
          SessionInfo.class, //8
          ConsumerInfo.class, //9
          BrowserInfo.class, //10
          ConsumerAck.class, //11
          JumboText.class //12


  };

  public Class getInitialPartialRequest(short type);

  /**
   * used only in client side
   * @param dis
   * @return
   * @throws IOException
   */
  public Serialization getObject(DataInputStream dis) throws IOException;

}
