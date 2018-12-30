package org.apache.superq.log;

public class LoggerFactory {
  public static Logger getLogger(Class loggedClass){
    return new Logger(loggedClass);
  }
}
