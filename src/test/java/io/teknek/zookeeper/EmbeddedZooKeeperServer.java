package io.teknek.zookeeper;

import io.teknek.daemon.TeknekDaemon;

import java.util.Properties;

import org.apache.curator.test.QuorumConfigBuilder;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.BeforeClass;

public class EmbeddedZooKeeperServer {
  public static TestingZooKeeperServer zookeeperTestServer ;
  
  /**
   * This class is named setupA because later on when setting up
   * a kafka server we call that setupB. This method seems to order
   * setup properly. Your mileage may vary.
   * @throws Exception
   */
  @BeforeClass
  public static void setupA() throws Exception{
    if (zookeeperTestServer == null){
      zookeeperTestServer = new TestingZooKeeperServer(new QuorumConfigBuilder());
      zookeeperTestServer.start();
    }
  }
  
  /**
   * 
   * @return a Teknek wired to this zk. Make sure to close when done
   * @throws InterruptedException
   */
  public static TeknekDaemon createDaemonWiredToThisZk() throws InterruptedException {
    Properties properties = new Properties();
    properties.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    final TeknekDaemon td = new TeknekDaemon(properties);
    td.init();
    return td;
  }
  
  
  public static TeknekDaemon createUnitiDaemonWiredToThisZk() throws InterruptedException {
    Properties properties = new Properties();
    properties.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getInstanceSpec().getConnectString());
    final TeknekDaemon td = new TeknekDaemon(properties);
    return td;
  }
}
