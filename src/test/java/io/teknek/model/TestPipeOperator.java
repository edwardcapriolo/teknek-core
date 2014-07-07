package io.teknek.model;

import io.teknek.collector.Collector;
import io.teknek.operator.PipeOperator;
import io.teknek.operator.SimplePipeOperator;
import io.teknek.util.MapBuilder;

import org.junit.Assert;
import org.junit.Test;

public class TestPipeOperator {

  @SuppressWarnings("unchecked")
  @Test
  public void simpleTest() throws InterruptedException {
    Operator o = new SimplePipeOperator();
    o.setProperties(
            MapBuilder.makeMap(PipeOperator.PIPE_OPERATOR_COMMAND, new String[] { "/bin/cat" }, 
                    SimplePipeOperator.INPUT_FIELD, "x",
                    SimplePipeOperator.OUTPUT_FIELD, "y"));
    Collector c = new Collector();
    o.setCollector(c);
    o.handleTuple(new Tuple().withField("x", "abc"));
    o.handleTuple(new Tuple().withField("x", "abc"));
    o.handleTuple(new Tuple().withField("x", "abc"));
    Assert.assertEquals(3, c.size());
    Thread.sleep(1);
    Tuple t = (Tuple) c.take();
    Assert.assertEquals("abc", t.getField("y"));
    o.close();
  }
}
