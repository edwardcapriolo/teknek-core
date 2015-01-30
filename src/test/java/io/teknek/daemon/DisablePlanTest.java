package io.teknek.daemon;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

public class DisablePlanTest extends EmbeddedZooKeeperServer {

  private TeknekDaemon td = null;
  private TeknekDaemon td1 = null;

  @Before
  public void before() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
    td.setRescanMillis(1000);
    td.init();
    
    td1 = new TeknekDaemon(props);
    td1.setRescanMillis(1000);
    td1.init();
  }

  public void sleep(long millis){
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }
  
  private int totalRunningThreadsForPlan(Plan p){
    int running = 0;
    if (td.workerThreads.get(p) != null) {
      running += td.workerThreads.get(p).size();
    }
    if (td1.workerThreads.get(p) != null) {
      running += td1.workerThreads.get(p).size();
    }
    return running;
  }
  @Test
  public void hangAround() throws InterruptedException {
    final Plan p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            100000))).withRootOperator(new OperatorDesc(new TenSecondOperator()));
    p.setName("shutup");
    p.setMaxWorkers(1);
    td.applyPlan(p);
    sleep(2000);
    Assert.assertNotNull(td.workerThreads);
    Assert.assertTrue(td.workerThreads.get(p) != null || td1.workerThreads.get(p) != null);
    
    Assert.assertEquals(1, totalRunningThreadsForPlan(p));
    p.setDisabled(true);
    td.applyPlan(p);
    sleep(5000);
    
    Assert.assertEquals(0, totalRunningThreadsForPlan(p));
    p.setDisabled(false);
    td.applyPlan(p);
    sleep(5000);
    Assert.assertEquals(1, totalRunningThreadsForPlan(p));
    
    p.setDisabled(true);
    td.applyPlan(p);
    Thread.sleep(2000);
    td.deletePlan(p);
    
  }

  @After
  public void after() {
    td.stop();
    td1.stop();
  }

}
