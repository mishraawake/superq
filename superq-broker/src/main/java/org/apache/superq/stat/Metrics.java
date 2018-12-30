package org.apache.superq.stat;

import java.util.Map;

public interface Metrics {
  String getName();
  Map<RollingMetrics, Stats> getStat();
}
