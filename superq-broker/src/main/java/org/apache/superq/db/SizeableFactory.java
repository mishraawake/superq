package org.apache.superq.db;

import org.apache.superq.Serialization;

public interface SizeableFactory<T extends Serialization> {
  T getSizeable();

  T getSizeableByType(short type);
}
