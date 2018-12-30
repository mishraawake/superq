package org.apache.superq.reqres;

import org.apache.superq.Serialization;
import org.apache.superq.network.ConnectionContext;

public interface RequestHandler<T extends Serialization> {
  void handle(T serialization, ConnectionContext connectionContext);
}
