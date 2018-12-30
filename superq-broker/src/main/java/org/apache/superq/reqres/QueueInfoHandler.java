package org.apache.superq.reqres;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.QueueInfo;
import org.apache.superq.Serialization;
import org.apache.superq.Task;
import org.apache.superq.io.IOAsyncUtil;
import org.apache.superq.network.ConnectionContext;

public class QueueInfoHandler implements RequestHandler<QueueInfo> {

  @Override
  public void handle(QueueInfo queueInfo, ConnectionContext connectionContext) {

    connectionContext.getBroker().enqueueFileIo(new Task() {
      @Override
      public void perform() throws Exception {
        try {
          connectionContext.getBroker().saveQueue(queueInfo);
        }
        catch (JMSException | IOException e) {
          (queueInfo).setQueueName(e.getMessage());
          e.printStackTrace();
        }
      }
    }, new Task() {
      @Override
      public void perform() throws Exception {
        connectionContext.sendAsyncPacket(queueInfo);
      }
    });

  }
}
