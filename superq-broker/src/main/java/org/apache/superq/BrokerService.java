package org.apache.superq;

import java.io.IOException;
import java.util.Map;

import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactory;
import org.apache.superq.db.FileDatabaseFactoryImpl;
import org.apache.superq.network.BrokerServer;

public class BrokerService {


  public void start() throws IOException {
    Broker broker = new Broker();
    broker.start();
    initializeQueue(broker);
    BrokerServer bs = new BrokerServer(1234, broker);
  }

  private void initializeQueue(Broker broker) throws IOException {
    Map<String, FileDatabaseFactory.QueueDatabase> databases = FileDatabaseFactoryImpl.getInstance().getAvailableInfoDbs();

    for (Map.Entry<String, FileDatabaseFactory.QueueDatabase> fileDatabaseEntry : databases.entrySet()) {
      String queueName = fileDatabaseEntry.getKey();
      FileDatabase<SMQMessage> mainDatabase = fileDatabaseEntry.getValue().getMainDB();
      FileDatabase<Serialization> infoDB = fileDatabaseEntry.getValue().getInfoDb();
    }
  }
}
