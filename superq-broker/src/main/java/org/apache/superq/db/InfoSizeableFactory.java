package org.apache.superq.db;

import org.apache.superq.ConsumerInfo;
import org.apache.superq.QueueInfo;
import org.apache.superq.Serialization;

public class InfoSizeableFactory<T extends Serialization> implements SizeableFactory<T> {
  @Override
  public T getSizeable() {
    return null;
  }

  @Override
  public T getSizeableByType(short type) {
    switch (type){
      case 7:
        return (T)new QueueInfo();
    }
    return null;
  }
}
