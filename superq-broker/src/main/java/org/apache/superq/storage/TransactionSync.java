package org.apache.superq.storage;

public interface TransactionSync {
  default void afterCommit(){}
  default void beforeCommit(){}
  default void afterRollback(){}
  default void beforeEnd(){}
}
