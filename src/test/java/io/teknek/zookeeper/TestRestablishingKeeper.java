package io.teknek.zookeeper;

import java.util.Properties;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.plan.Plan;
import junit.framework.Assert;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.QuorumConfigBuilder;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class TestRestablishingKeeper {
  
  @Test
  public void showAutoRecovery() throws Exception {
    TestingZooKeeperServer zookeeperTestServer = new TestingZooKeeperServer(new QuorumConfigBuilder());
    zookeeperTestServer.start();
    int port = zookeeperTestServer.getInstanceSpec().getPort();
    Properties p = new Properties();
    p.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    final TeknekDaemon td = new TeknekDaemon(p);
    RestablishingKeeper k = new RestablishingKeeper(zookeeperTestServer.getInstanceSpec().getConnectString()){
      @Override
      public void onReconnect(ZooKeeper zk, CuratorFramework framework){
        try {
          td.getWorkerDao().createZookeeperBase(zk);
        } catch (WorkerDaoException e) {
          throw new RuntimeException(e);
        }
      }
    };
    //Assert.assertEquals(1, k.getReestablished());
    Plan plan = new Plan().withName("abc");
    td.getWorkerDao().createOrUpdatePlan(plan, k.getZooKeeper());
    Assert.assertEquals(1, td.getWorkerDao().finalAllPlanNames(k.getZooKeeper()).size());
    zookeeperTestServer.close();
    try {
      td.getWorkerDao().finalAllPlanNames(k.getZooKeeper());
      Assert.fail("Dao should have failed");
    } catch (Exception ex){ }
    TestingZooKeeperServer revive = new TestingZooKeeperServer(new QuorumConfigBuilder(), port);
    try {
      Assert.assertEquals(1, td.getWorkerDao().finalAllPlanNames(k.getZooKeeper()).size());
      Assert.fail("Dao should have failed");
    } catch (Exception ex){
      
    }
    Thread.sleep(3000);
    Assert.assertEquals(0, td.getWorkerDao().finalAllPlanNames(k.getZooKeeper()).size());
    Assert.assertEquals(2, k.getReestablished());
    revive.close();
  }
}
