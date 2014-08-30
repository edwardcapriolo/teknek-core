package io.teknek.metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import io.teknek.driver.Driver;
import io.teknek.driver.DriverFactory;
import io.teknek.driver.TestDriver;
import io.teknek.feed.FeedPartition;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;

public class TestMetrics {

  public static FeedPartition getPart(){
    Map<String,Object> prop = new HashMap<String,Object>();
    int expectedPartitions = 2;
    int expectedRows = 10;
    prop.put(FixedFeed.NUMBER_OF_PARTITIONS, expectedPartitions);
    prop.put(FixedFeed.NUMBER_OF_ROWS, expectedRows);
    FixedFeed pf = new FixedFeed(prop);
    List<FeedPartition> parts = pf.getFeedPartitions();
    return parts.get(0);
  }
  
  public static Plan metricTestPlan(){
    Plan plan = new Plan()
    .withFeedDesc(new FeedDesc()
      .withFeedClass(FixedFeed.class.getName())
        .withProperties(MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,10)))
     .withRootOperator(new OperatorDesc(new OperatorWithMetrics()));
    return plan;
  }
  
  @Test
  public void simpleTest() throws InterruptedException {
    MetricRegistry mr = new MetricRegistry();
    Driver driver = DriverFactory.createDriver(getPart(), metricTestPlan(), mr);
    Thread t = new Thread(driver);
    t.start();
    t.join(1000000);
    Assert.assertEquals(10L, mr.counter("a.b").getCount());
  }
}
