package io.teknek.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class RestablishingKeeper implements Watcher {

  private String hostList;
  private volatile ZooKeeper zk;
  private volatile CountDownLatch awaitConnection;
  
  public RestablishingKeeper(String hostList) throws IOException, InterruptedException {
    this.hostList = hostList;
    reconnect();
  }

  public synchronized void reconnect() throws IOException, InterruptedException {
    if (zk !=null){
      try { zk.close();} catch (InterruptedException ex){}
    }
    zk = new ZooKeeper(hostList, 30000, this);
    awaitConnection = new CountDownLatch(1);
    awaitConnection.await(10, TimeUnit.SECONDS);
    onReconnect(this.zk);
  }
  
  public ZooKeeper getZooKeeper(){
    return zk;
  }
  
  public void onReconnect(ZooKeeper zk){
    
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected){
      awaitConnection.countDown();
    } else if (event.getState() == KeeperState.Expired || event.getState() == KeeperState.Disconnected){
      try {
        zk.close();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      try {
        reconnect();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
