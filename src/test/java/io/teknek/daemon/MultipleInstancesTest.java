package io.teknek.daemon;

import io.teknek.zookeeper.EmbeddedZooKeeperServer;

import java.util.Properties;


import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultipleInstancesTest extends EmbeddedZooKeeperServer {

  static TeknekDaemon base;
  static TeknekDaemon alternate;
  
  private static void setupBase() throws InterruptedException {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_BASE_DIR, "/base");
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    base = new TeknekDaemon(props);
    base.init();
  }
  
  private static void setupAlternate() throws InterruptedException {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    props.put(TeknekDaemon.ZK_BASE_DIR, "/alternate");
    alternate = new TeknekDaemon(props);
    alternate.init();
  }
  
  @BeforeClass
  public static void setup() throws InterruptedException {
    setupBase();
    setupAlternate();
  }
  
  @Test
  public void test() throws InterruptedException{
    Thread.sleep(5000);
    Assert.assertEquals("/base/workers", base.getWorkerDao().WORKERS_ZK);
    Assert.assertEquals(1, base.findAllWorkers().size());
    Assert.assertEquals("/alternate/workers", alternate.getWorkerDao().WORKERS_ZK);
    Assert.assertEquals(1, alternate.findAllWorkers().size());
  }
  
  @AfterClass
  public static void shutdown(){
    base.stop();
    alternate.stop();
  }
  
}
