package io.teknek.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class RestablishingKeeper implements Watcher {

  private final static Logger LOGGER = Logger.getLogger(RestablishingKeeper.class.getName());
  private String hostList;
  private volatile ZooKeeper zk;
  private volatile CountDownLatch awaitConnection;
  private long reestablished;
  
  public RestablishingKeeper(String hostList) throws IOException, InterruptedException {
    this.hostList = hostList;
    reconnect();
  }

  public synchronized void reconnect() throws IOException, InterruptedException {
    reestablished++;
    if (zk != null) {
      try {
        zk.close();
      } catch (InterruptedException ex) {
        LOGGER.warn(ex);
      }
    }
    zk = new ZooKeeper(hostList, 30000, this);
    awaitConnection = new CountDownLatch(1);
    awaitConnection.await(10, TimeUnit.SECONDS);
    onReconnect(zk);
  }
  
  public ZooKeeper getZooKeeper(){
    return zk;
  }
  
  /**
   * On reconnect execute these operations
   * @param zk
   */
  public void onReconnect(ZooKeeper zk){
    
  }
  
  @Override
  public void process(WatchedEvent event) {
    LOGGER.debug(event);
    if (event.getState() == KeeperState.SyncConnected) {
      awaitConnection.countDown();
    } else if (event.getState() == KeeperState.Expired || event.getState() == KeeperState.Disconnected){
      try {
        reconnect();
      } catch (IOException | InterruptedException ex) {
        LOGGER.warn(ex);
      }
    }
  }

  public long getReestablished() {
    return reestablished;
  }
  
}
