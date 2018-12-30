package org.apache.superq;

public class RollbackTransaction extends StartTransaction{

  public short getType(){
    return 5;
  }
}
