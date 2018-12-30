package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.superq.SMQMessage;
import org.apache.superq.Serialization;

public class FileDatabaseFactoryImpl<T extends Serialization> implements FileDatabaseFactory<Serialization> {

  Map<String, QueueDatabase> databases = new ConcurrentHashMap<>();

  String dbLocation;

  private static FileDatabaseFactory<Serialization> factory;


  static {
    factory = new FileDatabaseFactoryImpl<>("data/filedata");
    try {
      ((FileDatabaseFactoryImpl)factory).createDatabases();
    }
    catch (IOException e) {
     throw new RuntimeException("Problem in initializing the database ",e);
    }
  }

  private void createDatabases() throws IOException {
    File file = new File(dbLocation);
    if(!file.exists()){
      if(!Files.exists(Paths.get(dbLocation)))
        Files.createDirectories(Paths.get(dbLocation));
      return;
    }
    for(File dir : file.listFiles()){
      if(dir.isDirectory()){
        String dbName = dir.getName();
        QueueDatabase database = new QueueDatabase();
        database.infoDb = new FileDatabase<Serialization>(dbLocation +"/" + dbName + "/info", new InfoSizeableFactory());
        database.mainDB = new FileDatabase<SMQMessage>(dbLocation +"/" + dbName , new MessageSizeableFactory<SMQMessage>());
        databases.put(dbName, database);
      }
    }
  }

  public FileDatabaseFactoryImpl(String dbLocation){
    this.dbLocation = dbLocation;
  }

  @Override
  public FileDatabase<SMQMessage> getOrInitializeMainDatabase(String dbName) throws IOException  {
    FileDatabase mainDatabase = null;
    if(!databases.containsKey(dbName) || databases.get(dbName).mainDB == null){
      mainDatabase = new FileDatabase<SMQMessage>(dbLocation + "/"+dbName, new MessageSizeableFactory<SMQMessage>());
    } else {
      return databases.get(dbName).mainDB;
    }

    if(!databases.containsKey(dbName)){
      databases.put(dbName, new QueueDatabase());
    }

    databases.get(dbName).mainDB = mainDatabase;

    return mainDatabase;
  }

  @Override
  public synchronized FileDatabase<Serialization> getInfoDatabase(String dbName, SizeableFactory<Serialization> factory) throws IOException {
    FileDatabase infoDatabase = null;
    if(!databases.containsKey(dbName) || databases.get(dbName).infoDb == null){
      infoDatabase = new FileDatabase<Serialization>(dbLocation + "/"+dbName +"/info", new InfoSizeableFactory<Serialization>());
      infoDatabase.setTyped();
    } else {
      return databases.get(dbName).infoDb;
    }

    if(!databases.containsKey(dbName)){
      databases.put(dbName, new QueueDatabase());
    }

    databases.get(dbName).infoDb = infoDatabase;

    return infoDatabase;
  }

  @Override
  public Map<String, QueueDatabase> getAvailableInfoDbs() throws IOException {
    return databases;
  }

  public static FileDatabaseFactory getInstance(){
    return factory;
  }
}
