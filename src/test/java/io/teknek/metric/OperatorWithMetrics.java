package io.teknek.metric;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

public class OperatorWithMetrics extends Operator {

  @Override
  public void handleTuple(ITuple tuple) {
    getMetricRegistry().counter("a.b").inc();
    getMetricRegistry().counter(getPath() + ".processed").inc();
    getMetricRegistry().counter(getPath() + "." + this.getPartitionId() + ".processed").inc();
  }
  
}
