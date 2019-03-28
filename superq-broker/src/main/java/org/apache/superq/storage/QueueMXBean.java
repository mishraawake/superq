package org.apache.superq.storage;

public interface QueueMXBean {
   int getNumberOfConsumers();
   boolean isMoreDataPresent();
   boolean isLoading();
   int getMemorySize();
}
