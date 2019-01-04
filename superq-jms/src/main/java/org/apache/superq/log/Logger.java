package org.apache.superq.log;

public class Logger {

  private Class loggedClass;

  public Logger(Class loggedClass){
    this.loggedClass = loggedClass;
  }

  public void errorLog(String log, Object ... params){
    System.out.println(log);
  }

  public void fatalLog(String log, Object ... params){

  }

  public void warnLog(String log, Object ... params){

  }

  public void infoLog(String log, Object ... params){

  }

  public void debugLog(String log, Object ... params){

  }

  public boolean ifInfo(){
    return true;
  }

  public boolean ifDebug(){
    return true;
  }
}
