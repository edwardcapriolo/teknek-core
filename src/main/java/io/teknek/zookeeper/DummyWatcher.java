package io.teknek.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * DummyWatcher has a countdown latch that attempts to make sure you are 
 * connected to zookeeper. 
 * @author edward
 *
 */
public class DummyWatcher implements Watcher {

  private CountDownLatch awaitConnection;
  
  public DummyWatcher(){
    awaitConnection = new CountDownLatch(1);
  }
  
  public boolean connectOrThrow(long timeout, TimeUnit unit) throws InterruptedException{
    return awaitConnection.await(timeout, unit);
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected){
      awaitConnection.countDown();
    } 
  }

}
