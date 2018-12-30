package org.apache.superq.outgoing;

import org.apache.superq.stat.Metrics;

// design problem: if we want to introduce another type of metrics with varying dimension.
// e.g cpu consumed, and dimension is only the max and min.
public interface ConsumerMetrics {
  Metrics getTimingMetrics();
  Metrics getMemoryMetrics();
  Metrics getMessageSizeMetrics();
  Metrics getOnWireSendTime();
}
