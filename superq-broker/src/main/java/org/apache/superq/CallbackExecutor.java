package org.apache.superq;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CallbackExecutor implements Runnable {

  private BlockingQueue<Task> callbackTasks;

  public CallbackExecutor(){
    callbackTasks = new LinkedBlockingQueue<>();
  }

  @Override
  public void run() {
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
