package org.apache.superq.outgoing;

import java.util.List;
import java.util.Set;

import org.apache.superq.ConsumerInfo;
import org.apache.superq.datatype.MessageGroup;
import org.apache.superq.storage.Constraint;

public interface SBConsumer<M> {
  ConsumerInfo getConsumerInfo();
  void dispatch(M message);
  void prepare();
  void start();
  void dispose();
  M pull();
  void ack(long messageId);
  int outstandingAcks();
  List<Constraint> getConstraints();
  void addConstraint(Constraint constraint);
  ConsumerStatus getConsumerStatus();
  boolean canAcceptMoreMessage();
  MessageGroup associatedMessageGroup();
  boolean matches(M message);
  Set<String> ownedGroups();
  void assignGroup(String groupId);
  void groupEnd();
  void unAssignGroup();
  void ownGroup(String groupId);
}
