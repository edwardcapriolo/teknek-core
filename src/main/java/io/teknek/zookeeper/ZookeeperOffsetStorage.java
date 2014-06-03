package io.teknek.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import io.teknek.feed.FeedPartition;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;
import io.teknek.plan.Plan;

public class ZookeeperOffsetStorage extends OffsetStorage {
  
  public static final String ENCODING = "UTF-8";
  
  public static final String TEKNEK_ROOT = "/teknek";

  public static final String TEKNEK_OFFSET = TEKNEK_ROOT + "/offset";

  public static final String ZK_CONNECT = "zookeeper.connect";
  
  public ZookeeperOffsetStorage(FeedPartition feedPartition, Plan plan, Map<String,String> properties) {
    super(feedPartition, plan, properties);
  }
  
  @Override
  public void persistOffset(Offset o) {
    createZookeeperBase();
    ZooKeeper zk = null;
    String s = TEKNEK_OFFSET + "/" + plan.getName() + "-" + feedPartiton.getFeed().getName()+ "-" + feedPartiton.getPartitionId();
    try {
      DummyWatcher dw = new DummyWatcher();
      zk = new ZooKeeper(properties.get(ZK_CONNECT), 100, dw);
      dw.connectOrThrow(10, TimeUnit.SECONDS);
      Stat stat = zk.exists(s, true);
      if (stat != null) {
        zk.setData(s, o.serialize(), stat.getVersion());
      } else {
        zk.create(s, o.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.close();
    } catch (IOException | KeeperException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }
 
  @Override
  public Offset getCurrentOffset(){
      ZookeeperOffset zko;
      try {
        zko = new ZookeeperOffset(feedPartiton.getOffset().getBytes(ENCODING));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException ("should be " + ZookeeperOffsetStorage.ENCODING, e);
      }
      return zko;
  }

  @Override
  public Offset findLatestPersistedOffset() {
    String s = TEKNEK_OFFSET + "/" + plan.getName() + "-" + feedPartiton.getFeed().getName()+ "-" + feedPartiton.getPartitionId();
    ZooKeeper zk = null;
    try {
      DummyWatcher dw = new DummyWatcher();
      zk = new ZooKeeper(properties.get(ZK_CONNECT), 100, dw);
      dw.connectOrThrow(10, TimeUnit.SECONDS);
      Stat stat = zk.exists(s, true);
      
      if (stat != null) {
        byte [] bytes = zk.getData(s, true, stat);
        ZookeeperOffset zo = new ZookeeperOffset(bytes);
        zk.close();
        return zo;
      } else {
        return null;
      }
    } catch (IOException | KeeperException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }

  public void createZookeeperBase() {
    DummyWatcher dw = new DummyWatcher();
    try {
      ZooKeeper zk = new ZooKeeper(properties.get(ZK_CONNECT), 100, new DummyWatcher());
      dw.connectOrThrow(10, TimeUnit.SECONDS);
      if (zk.exists(TEKNEK_ROOT, true) == null) {
        zk.create(TEKNEK_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists(TEKNEK_OFFSET, true) == null) {
        zk.create(TEKNEK_OFFSET, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.close();
    } catch (KeeperException  | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    } 
  }

}

class ZookeeperOffset extends Offset {

  private String offset;
  
  public ZookeeperOffset(byte[] bytes) {
    super(bytes);
    try {
      offset = new String(bytes, ZookeeperOffsetStorage.ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException ("should be " + ZookeeperOffsetStorage.ENCODING, e);
    }
  }
  
  @Override
  public byte[] serialize() {
    try {
      return offset.getBytes(ZookeeperOffsetStorage.ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException ("should be " + ZookeeperOffsetStorage.ENCODING, e);
    }
  }
  
}
