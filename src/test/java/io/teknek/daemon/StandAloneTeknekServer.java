package io.teknek.daemon;

import io.teknek.zookeeper.EmbeddedZooKeeperServer;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class StandAloneTeknekServer extends EmbeddedZooKeeperServer {

  static TeknekDaemon td = null;
  
  @BeforeClass
  public static void setup() throws InterruptedException {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    td = new TeknekDaemon(props);
    td.init();
    System.out.println("started zk on " + zookeeperTestServer.getInstanceSpec().getConnectString());
  }
  
  @Ignore
  //@Test
  public void hangAround(){
    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  @AfterClass
  public static void shutdown(){
    td.stop();
  }
}
