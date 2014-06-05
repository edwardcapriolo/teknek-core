package io.teknek.datalayer;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import io.teknek.daemon.WorkerStatus;
import io.teknek.driver.TestDriverFactory;
import io.teknek.plan.Bundle;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.zookeeper.DummyWatcher;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

public class TestWorkerDao extends EmbeddedZooKeeperServer {
  
  @Test
  public void persistAndReadBack() throws IOException, InterruptedException, WorkerDaoException {
    String group = "io.teknek";
    String name = "abc";
    DummyWatcher dw = new DummyWatcher();
    ZooKeeper zk = new ZooKeeper(zookeeperTestServer.getConnectString(), 1000, dw);
    dw.connectOrThrow(10, TimeUnit.SECONDS);
    OperatorDesc d = TestDriverFactory.buildGroovyOperatorDesc();
    WorkerDao.createZookeeperBase(zk);
    WorkerDao.saveOperatorDesc(zk, d, group, name);
    OperatorDesc d1 = WorkerDao.loadSavedOperatorDesc(zk, group, name);
    Assert.assertEquals(d1.getTheClass(), d.getTheClass());
    Assert.assertEquals(d1.getSpec(), d.getSpec());
    Assert.assertEquals(d1.getScript(), d.getScript());
  }

  
  @Test
  public void readBundleFromUrl() throws MalformedURLException, WorkerDaoException{
    File f = new File("src/test/resources/bundle_io.teknek_itests1.0.0.json");
    Bundle b = WorkerDao.getBundleFromUrl(f.toURL());
    Assert.assertEquals("itests", b.getBundleName() );
    Assert.assertEquals("groovy_identity", b.getOperatorList().get(0).getName());
  }
  
  @Test
  public void readBundleAndAdd() throws IOException, InterruptedException, WorkerDaoException{
    File f = new File("src/test/resources/bundle_io.teknek_itests1.0.0.json");
    Bundle b = WorkerDao.getBundleFromUrl(f.toURL());
    DummyWatcher dw = new DummyWatcher();
    ZooKeeper zk = new ZooKeeper(zookeeperTestServer.getConnectString(), 100, dw);
    dw.connectOrThrow(10, TimeUnit.SECONDS);
    WorkerDao.saveBundle(zk, b);
    OperatorDesc oDesc = WorkerDao.loadSavedOperatorDesc(zk, b.getPackageName(), "groovy_identity");
    Assert.assertEquals("groovy_identity", oDesc.getTheClass());
    FeedDesc fDesc = WorkerDao.loadSavedFeedDesc(zk, b.getPackageName(), "GTry");
    Assert.assertEquals("GTry", fDesc.getTheClass());
  }
 
  @Test
  public void persistStatus() throws WorkerDaoException, IOException, InterruptedException{
    DummyWatcher dw = new DummyWatcher();
    ZooKeeper zk = new ZooKeeper(zookeeperTestServer.getConnectString(), 100, dw);
    dw.connectOrThrow(10, TimeUnit.SECONDS);
    WorkerStatus ws = new WorkerStatus("1","2","3");
    Plan p = new Plan().withName("persist");
    WorkerDao.createZookeeperBase(zk);
    WorkerDao.createOrUpdatePlan(p, zk);
    WorkerDao.registerWorkerStatus(zk, p , ws);
    List<WorkerStatus> statuses = WorkerDao.findAllWorkerStatusForPlan(zk, p, WorkerDao.findWorkersWorkingOnPlan(zk, p));
    Assert.assertEquals(1, statuses.size());
    Assert.assertEquals(ws.getTeknekDaemonId(), statuses.get(0).getTeknekDaemonId());
    Assert.assertEquals(ws.getFeedPartitionId(), statuses.get(0).getFeedPartitionId());
    Assert.assertEquals(ws.getWorkerUuid(), statuses.get(0).getWorkerUuid());
    zk.close();
    zk = new ZooKeeper(zookeeperTestServer.getConnectString(), 100, dw);
    WorkerDao.deletePlan(zk, p);
    zk.close();
  }
}
