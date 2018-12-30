package org.apache.superq.outgoing;

public interface ConsumerStatus {
  int getUnAckMessages();
  int memory();
  int getTime();
  int getTotalMessages();
  ConsumerMetrics getConsumptionRate();
}
