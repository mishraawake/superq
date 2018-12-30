package org.apache.superq;

public class CommitTransaction extends StartTransaction {

  public short getType(){
    return 4;
  }
}
