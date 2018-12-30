package org.apache.superq.db;

import org.apache.superq.Serialization;

public class EntrySizeableFactory<T extends Serialization> implements SizeableFactory<IndexEntry> {
  @Override
  public IndexEntry getSizeable() {
    return new IndexEntry();
  }

  @Override
  public IndexEntry getSizeableByType(short type) {
    return null;
  }
}
