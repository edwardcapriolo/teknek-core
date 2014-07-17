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

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.teknek.datalayer.WorkerDao;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.DummyWatcher;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

/**
 * This is a really cruddy end to end test. We should make a unit test.
 * @author edward
 *
 */
public class BadPlanNameTest extends EmbeddedZooKeeperServer {

  TeknekDaemon td = null;

  Plan p;

  @Before
  public void setup() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
    td.init();
  }

  /**
   * Creates a plan that has a different node name then plan name
   * @param plan
   * @param zk
   * @throws WorkerDaoException
   */
  public static void createOrUpdateBadPlan(Plan plan, ZooKeeper zk) throws WorkerDaoException {
    try {
      WorkerDao.createZookeeperBase(zk);
      Stat s = zk.exists(WorkerDao.PLANS_ZK + "/" + plan.getName() + "a", false);
      if (s != null) {
        zk.setData(WorkerDao.PLANS_ZK + "/" + plan.getName() + "a", WorkerDao.serializePlan(plan),
                s.getVersion());
      } else {
        zk.create(WorkerDao.PLANS_ZK + "/" + plan.getName() + "a", WorkerDao.serializePlan(plan),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  @Test
  public void hangAround() throws IOException, WorkerDaoException {
    p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(new OperatorDesc(new BeLoudOperator()));
    p.setName("yell");
    p.setMaxWorkers(1);
    
    DummyWatcher dw = new DummyWatcher();
    ZooKeeper zk = new ZooKeeper(zookeeperTestServer.getConnectString(),200, dw, false);
    createOrUpdateBadPlan(p, zk);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @After
  public void shutdown() {
    td.deletePlan(p);
    td.stop();
  }
}
