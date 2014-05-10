package io.teknek.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;

import junit.framework.Assert;

import io.teknek.collector.Collector;
import io.teknek.util.MapBuilder;

import org.junit.Test;

public class TestPipeOperator {

  @Test
  public void simpleTest() throws InterruptedException {
    PipeOperator o = new PipeOperator() {
      BufferedReader br;
      BufferedWriter bw;

      @Override
      public void setProperties(Map<String, Object> properties) {
        super.setProperties(properties);
        br = new BufferedReader(new InputStreamReader(this.output));
        bw = new BufferedWriter(new OutputStreamWriter(this.toProcess));
      }

      @Override
      public void handleTuple(ITuple tuple) {
        try {
          bw.write(tuple.getField("x").toString() + "\n");
          bw.flush();
          collector.emit(new Tuple().withField("y", br.readLine()));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    };
    o.setProperties(MapBuilder.makeMap(PipeOperator.PIPE_OPERATOR_COMMAND,
            new String[] { "/bin/cat" }));
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
