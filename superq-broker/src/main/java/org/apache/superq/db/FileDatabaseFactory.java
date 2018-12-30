package org.apache.superq.db;

import java.io.IOException;
import java.util.Map;

import org.apache.superq.SMQMessage;
import org.apache.superq.Serialization;

public interface FileDatabaseFactory<T extends Serialization> {
  FileDatabase<SMQMessage> getOrInitializeMainDatabase(String dbName) throws IOException;
  FileDatabase<T> getInfoDatabase(String dbName, SizeableFactory<T> factory) throws IOException;
  Map<String, FileDatabaseFactoryImpl.QueueDatabase> getAvailableInfoDbs() throws IOException;

  public static class QueueDatabase {
    FileDatabase<SMQMessage> mainDB;
    FileDatabase<Serialization> infoDb;

    public FileDatabase<SMQMessage> getMainDB() {
      return mainDB;
    }

    public void setMainDB(FileDatabase<SMQMessage> mainDB) {
      this.mainDB = mainDB;
    }

    public FileDatabase<Serialization> getInfoDb() {
      return infoDb;
    }

    public void setInfoDb(FileDatabase<Serialization> infoDb) {
      this.infoDb = infoDb;
    }
  }
}
