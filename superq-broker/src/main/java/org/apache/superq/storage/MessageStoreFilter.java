package org.apache.superq.storage;

import org.apache.superq.Serialization;

public interface MessageStoreFilter<M extends Serialization> extends MessageStore<M> {
}
