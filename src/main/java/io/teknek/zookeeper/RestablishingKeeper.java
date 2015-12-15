package io.teknek.zookeeper;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

public abstract class RestablishingKeeper {

  private final static Logger LOGGER = Logger.getLogger(RestablishingKeeper.class.getName());
  private CuratorFramework client;
  private AtomicLong reEstablished = new AtomicLong(0);
  
  public RestablishingKeeper(String hostList)  {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
    client = CuratorFrameworkFactory.newClient(hostList, retryPolicy);
    client.getConnectionStateListenable().addListener(new ConnectionStateListener(){
      @Override
      public void stateChanged(CuratorFramework framework, ConnectionState state) {
        LOGGER.debug("State change "+ state);
        if (state.equals(ConnectionState.CONNECTED) || state.equals(ConnectionState.RECONNECTED)){
          reEstablished.incrementAndGet();
          try {
            onReconnect(framework.getZookeeperClient().getZooKeeper(), framework);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }});
  }
  
  public void init() throws InterruptedException {
    client.start();
    client.blockUntilConnected();
  }

  /**
   * On reconnect execute these operations
   * @param zk
   */
  public abstract void onReconnect(ZooKeeper zk, CuratorFramework framework);
  
  public ZooKeeper getZooKeeper(){
    try {
      return client.getZookeeperClient().getZooKeeper();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public CuratorFramework getCuratorFramework(){
    return client;
  }
  
  public long getReestablished(){
    return reEstablished.get();
  }
  
}
