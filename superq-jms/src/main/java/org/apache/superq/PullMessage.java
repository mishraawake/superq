package org.apache.superq;

import java.util.Arrays;

public class PullMessage extends ConsumerInfo {

  public short getType(){
    return 13;
  }

  @Override
  public String toString() {
    return "PullMessage{" +
            "qid=" + qid +
            ", isAsync=" + isAsync +
            ", fieldArray=" + fieldArray +
            ", deserializingIndex=" + deserializingIndex +
            ", bytes=" + Arrays.toString(bytes) +
            '}';
  }
}
