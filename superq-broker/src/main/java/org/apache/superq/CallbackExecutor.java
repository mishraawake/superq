package org.apache.superq;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CallbackExecutor implements Runnable {

  private BlockingQueue<Task> callbackTasks;
  private ThreadLocal<Boolean> callbackThread;

  public CallbackExecutor(ThreadLocal<Boolean> callbackThread){
    callbackTasks = new LinkedBlockingQueue<>();
    this.callbackThread = callbackThread;
  }

  @Override
  public void run() {
    callbackThread.set(true);
    Task task = null;
    try {
      while ((task = callbackTasks.take()) != null) {
        try {
          task.perform();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (InterruptedException i){
      i.printStackTrace();
    }
  }

  public void addTask(Task task){
    callbackTasks.add(task);
  }
}
