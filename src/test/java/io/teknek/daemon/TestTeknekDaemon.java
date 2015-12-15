/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.daemon;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTeknekDaemon extends EmbeddedZooKeeperServer {

  static TeknekDaemon td = null;
  static Plan p;
  
  @BeforeClass
  public static void setup() throws InterruptedException {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    td = new TeknekDaemon(props);
    td.init();
  }
   
  @Test
  public void maxWorkerTest(){
    Plan aPlan = new Plan().withMaxWorkersPerNode(0).withMaxWorkers(2);
    Worker workingOn1 = new Worker(aPlan, null, null);
    Worker workingOn2 = new Worker(aPlan, null, null);
    List<String> workerIds = Arrays.asList(workingOn1.getMyId().toString(), workingOn2.getMyId().toString());
    List<Worker> localWorkers = Arrays.asList(workingOn1,workingOn2);
    Assert.assertFalse(td.alreadyAtMaxWorkersPerNode(aPlan, workerIds, localWorkers));
    aPlan.setMaxWorkersPerNode(2);
    Assert.assertTrue(td.alreadyAtMaxWorkersPerNode(aPlan, workerIds, localWorkers));
    Assert.assertFalse(td.alreadyAtMaxWorkersPerNode(aPlan, workerIds, null));
  }
  
  @Test 
  public void testAcceptablePlans(){
    Assert.assertFalse(td.isPlanSane(null));
    Assert.assertFalse(td.isPlanSane(new Plan()));
    Plan plan = new Plan().withFeedDesc(new FeedDesc());
    Assert.assertTrue(td.isPlanSane(plan));
    plan.setDisabled(false);
    Assert.assertTrue(td.isPlanSane(plan));
  }
  
  @Test
  public void testBadConfig() throws InterruptedException{
    p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(new OperatorDesc(new BeLoudOperator()));
    p.setName("tooManyWorkers");
    p.setMaxWorkers(3);
    td.applyPlan(p);
    Thread.sleep(5000);
  }
  
  @AfterClass
  public static void shutdown() throws InterruptedException{
    p.setDisabled(true);
    td.applyPlan(p);
    Thread.sleep(1000);
    td.deletePlan(p);
    td.stop();
  }
  
}
