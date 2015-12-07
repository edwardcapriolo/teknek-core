package io.teknek.zookeeper;

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
}
