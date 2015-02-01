package io.teknek.zookeeper;

import io.teknek.datalayer.WorkerDao;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.plan.Plan;
import junit.framework.Assert;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import com.netflix.curator.test.TestingServer;

public class TestRestablishingKeeper {

  @Test
  public void showAutoRecovery() throws Exception {
    TestingServer zookeeperTestServer = new TestingServer();
    int port = zookeeperTestServer.getPort();
    RestablishingKeeper k = new RestablishingKeeper(zookeeperTestServer.getConnectString()){
      public void onReconnect(ZooKeeper zooKeeper){
        try {
          WorkerDao.createZookeeperBase(zooKeeper);
        } catch (WorkerDaoException e) {
          throw new RuntimeException(e);
        }
      }
    };
    Assert.assertEquals(1, k.getReestablished());
    Plan plan = new Plan().withName("abc");
    WorkerDao.createOrUpdatePlan(plan, k.getZooKeeper());
    Assert.assertEquals(1, WorkerDao.finalAllPlanNames(k.getZooKeeper()).size());
    zookeeperTestServer.close();
    try {
      WorkerDao.finalAllPlanNames(k.getZooKeeper());
      Assert.fail("Dao should have failed");
    } catch (Exception ex){ }
    TestingServer revive = new TestingServer(port);
    try {
      Assert.assertEquals(1, WorkerDao.finalAllPlanNames(k.getZooKeeper()).size());
      Assert.fail("Dao should have failed");
    } catch (Exception ex){
      
    }
    Thread.sleep(1000);
    Assert.assertEquals(0, WorkerDao.finalAllPlanNames(k.getZooKeeper()).size());
    Assert.assertEquals(2, k.getReestablished());
    revive.close();
  }
}
