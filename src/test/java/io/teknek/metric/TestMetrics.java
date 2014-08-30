package io.teknek.metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import io.teknek.driver.Driver;
import io.teknek.driver.DriverFactory;
import io.teknek.feed.FeedPartition;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;

public class TestMetrics {

  public static List<FeedPartition> getPart(){
    Map<String,Object> prop = new HashMap<String,Object>();
    int expectedPartitions = 2;
    int expectedRows = 10;
    prop.put(FixedFeed.NUMBER_OF_PARTITIONS, expectedPartitions);
    prop.put(FixedFeed.NUMBER_OF_ROWS, expectedRows);
    FixedFeed pf = new FixedFeed(prop);
    return pf.getFeedPartitions();
  }
  
  public static Plan metricTestPlan(){
    Plan plan = new Plan()
    .withFeedDesc(new FeedDesc()
      .withFeedClass(FixedFeed.class.getName())
        .withProperties(MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,10)))
     .withRootOperator(new OperatorDesc(new OperatorWithMetrics()));
    plan.setName("simple");
    return plan;
  }
  
  @Test
  public void simpleTest() throws InterruptedException {
    MetricRegistry mr = new MetricRegistry();
    {
      Driver driver = DriverFactory.createDriver(getPart().get(0), metricTestPlan(), mr);
      Thread t = new Thread(driver);
      t.start();
      t.join(1000000);
    }
    Assert.assertEquals(10L, mr.counter("a.b").getCount());
    Assert.assertEquals(10L, mr.counter("simple.OperatorWithMetrics.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.OperatorWithMetrics.0.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.FixedFeed.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.FixedFeed.0.processed").getCount());
    {
      Driver driver = DriverFactory.createDriver(getPart().get(1), metricTestPlan(), mr);
      Thread t = new Thread(driver);
      t.start();
      t.join(1000000);
    }
    Assert.assertEquals(20L, mr.counter("a.b").getCount());
    Assert.assertEquals(20L, mr.counter("simple.OperatorWithMetrics.processed").getCount());
    Assert.assertEquals(20L, mr.counter("simple.FixedFeed.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.OperatorWithMetrics.0.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.FixedFeed.0.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.OperatorWithMetrics.1.processed").getCount());
    Assert.assertEquals(10L, mr.counter("simple.FixedFeed.1.processed").getCount());
  }
}
