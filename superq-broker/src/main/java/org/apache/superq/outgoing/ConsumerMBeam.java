package org.apache.superq.outgoing;

public interface ConsumerMBeam {
  int getUnAcks();
  int getTotalRelayed();
  boolean isSync();
  boolean isPendingPull();
}
