package io.teknek.zookeeper;

import java.util.Properties;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.plan.Plan;
import junit.framework.Assert;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.QuorumConfigBuilder;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class TestRestablishingKeeper extends EmbeddedZooKeeperServer {
  
  @Test
  public void showAutoRecovery() throws Exception {
    Properties p = new Properties();
    p.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    final TeknekDaemon td = new TeknekDaemon(p);
    int port = zookeeperTestServer.getInstanceSpec().getPort();
    System.out.println(zookeeperTestServer.getInstanceSpec().getConnectString());
    td.init();
    //Assert.assertEquals(1, k.getReestablished());
    Plan plan = new Plan().withName("abc");
    td.getWorkerDao().createOrUpdatePlan(plan, td.getReestablishingKeeper().getZooKeeper());
    Assert.assertEquals(1, td.getWorkerDao().finalAllPlanNames(td.getReestablishingKeeper().getCuratorFramework()).size());
    zookeeperTestServer.stop();
    try {
      td.getWorkerDao().finalAllPlanNames(td.getReestablishingKeeper().getCuratorFramework());
      Assert.fail("Dao should have failed");
    } catch (Exception ex){ }
    try {
      Assert.assertEquals(1, td.getWorkerDao().finalAllPlanNames(td.getReestablishingKeeper().getCuratorFramework()).size());
      Assert.fail("Dao should have failed");
    } catch (Exception ex){ }
    zookeeperTestServer.start();
    Thread.sleep(3000);
    Assert.assertEquals(0, td.getWorkerDao().finalAllPlanNames(td.getReestablishingKeeper().getCuratorFramework()).size());
    Assert.assertEquals(2, td.getReestablishingKeeper().getReestablished());
    
  }
}
