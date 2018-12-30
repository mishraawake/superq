package org.apache.superq.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.superq.Task;
import org.apache.superq.network.BrokerServer;

public class IOAsyncUtil {

  static IOExecutor executor = new IOExecutor() {
    ExecutorService ex = Executors.newFixedThreadPool(3);
    @Override
    public Future submit(Runnable task) {
      return ex.submit(task);
    }
  };
  public static Future enqueueFileIo(Task perform, Task callback){
    return executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          perform.perform();
          callback.perform();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  private interface IOExecutor {
    Future submit(Runnable task);
  }

}
