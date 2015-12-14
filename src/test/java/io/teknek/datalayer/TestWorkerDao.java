package io.teknek.datalayer;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.daemon.WorkerStatus;
import io.teknek.driver.TestDriverFactory;
import io.teknek.plan.Bundle;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

public class TestWorkerDao extends EmbeddedZooKeeperServer {
  
  @Test
  public void persistAndReadBack() throws IOException, InterruptedException, WorkerDaoException {
    final TeknekDaemon td = createDaemonWiredToThisZk();
    String group = "io.teknek";
    String name = "abc";
    OperatorDesc d = TestDriverFactory.buildGroovyOperatorDesc();
    td.getWorkerDao().createZookeeperBase();
    td.getWorkerDao().saveOperatorDesc(td.getReestablishingKeeper().getZooKeeper(), d, group, name);
    OperatorDesc d1 = td.getWorkerDao().loadSavedOperatorDesc(td.getReestablishingKeeper().getZooKeeper(), group, name);
    Assert.assertEquals(d1.getTheClass(), d.getTheClass());
    Assert.assertEquals(d1.getSpec(), d.getSpec());
    Assert.assertEquals(d1.getScript(), d.getScript());
    td.stop();
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
    final TeknekDaemon td = createDaemonWiredToThisZk();
    File f = new File("src/test/resources/bundle_io.teknek_itests1.0.0.json");
    Bundle b = WorkerDao.getBundleFromUrl(f.toURL());
    td.getWorkerDao().saveBundle(td.getReestablishingKeeper().getZooKeeper(), b);
    OperatorDesc oDesc = td.getWorkerDao().loadSavedOperatorDesc(td.getReestablishingKeeper().getZooKeeper(), b.getPackageName(), "groovy_identity");
    Assert.assertEquals("groovy_identity", oDesc.getTheClass());
    FeedDesc fDesc = td.getWorkerDao().loadSavedFeedDesc(b.getPackageName(), "GTry");
    Assert.assertEquals("GTry", fDesc.getTheClass());
    td.stop();
  }
 
  @Ignore
  public void persistStatus() throws WorkerDaoException, IOException, InterruptedException {
    final TeknekDaemon td = createUnitiDaemonWiredToThisZk();
    WorkerStatus ws = new WorkerStatus("1","2","3");
    Plan p = new Plan().withName("persist");
    td.getWorkerDao().createZookeeperBase();
    td.getWorkerDao().createOrUpdatePlan(p);
    td.getWorkerDao().registerWorkerStatus(td.getReestablishingKeeper().getZooKeeper(), p , ws);
    List<WorkerStatus> statuses = td.getWorkerDao()
            .findAllWorkerStatusForPlan(p, td.getWorkerDao().findWorkersWorkingOnPlan(p));
    Assert.assertEquals(1, statuses.size());
    Assert.assertEquals(ws.getTeknekDaemonId(), statuses.get(0).getTeknekDaemonId());
    Assert.assertEquals(ws.getFeedPartitionId(), statuses.get(0).getFeedPartitionId());
    Assert.assertEquals(ws.getWorkerUuid(), statuses.get(0).getWorkerUuid());
    td.getWorkerDao().deletePlan(p);
    td.stop();
  }
}
