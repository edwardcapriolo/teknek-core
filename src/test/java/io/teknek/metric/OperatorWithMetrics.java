package io.teknek.metric;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

public class OperatorWithMetrics extends Operator {

  @Override
  public void handleTuple(ITuple tuple) {
    getMetricRegistry().meter("a.b").mark();
    getMetricRegistry().meter(getPath() + ".processed").mark();
    getMetricRegistry().meter(getPath() + "." + this.getPartitionId() + ".processed").mark();
  }
  
}
