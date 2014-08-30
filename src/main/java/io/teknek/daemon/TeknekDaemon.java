/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.daemon;

import io.teknek.datalayer.WorkerDao;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.graphite.reporter.SimpleJmxReporter;
import io.teknek.plan.Plan;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;

public class TeknekDaemon implements Watcher{

  private final static Logger logger = Logger.getLogger(TeknekDaemon.class.getName());
  public static final String ZK_SERVER_LIST = "teknek.zk.servers";
  public static final String MAX_WORKERS = "teknek.max.workers";
  public static final String DAEMON_ID = "teknek.daemon.id";
  
  private int maxWorkers = 4;
  private String myId;
  private Properties properties;
  private ZooKeeper zk;
  private long rescanMillis = 5000;
  ConcurrentHashMap<Plan, List<Worker>> workerThreads;
  private boolean goOn = true;
  private String hostname;
  private CountDownLatch awaitConnection;
  private MetricRegistry metricRegistry;
  private SimpleJmxReporter jmxReporter;
  
  public TeknekDaemon(Properties properties){
    this.properties = properties;
    if (properties.containsKey(DAEMON_ID)){
      myId = properties.getProperty(DAEMON_ID);
    } else {
      myId = UUID.randomUUID().toString();
    }
    workerThreads = new ConcurrentHashMap<Plan,List<Worker>>();
    if (properties.containsKey(MAX_WORKERS)){
      maxWorkers = Integer.parseInt(properties.getProperty(MAX_WORKERS));
    }
    try {
      setHostname(InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException ex) {
      setHostname("unknown");
    }
    metricRegistry = new MetricRegistry();
    jmxReporter = new SimpleJmxReporter(metricRegistry, "io.teknek.teknekcore");
  }
  
  public void init() {
    jmxReporter.init();
    logger.info("Daemon id:" + myId);
    logger.info("Connecting to:" + properties.getProperty(ZK_SERVER_LIST));
    awaitConnection = new CountDownLatch(1);
    try {
      zk = new ZooKeeper(properties.getProperty(ZK_SERVER_LIST), 1000, this);
      boolean connected = awaitConnection.await(10, TimeUnit.SECONDS);
      if (!connected){
        throw new RuntimeException("Did not connect before timeout");
      }
    } catch (IOException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
    try {
      WorkerDao.createZookeeperBase(zk);
      WorkerDao.createEphemeralNodeForDaemon(zk, this);
    } catch (WorkerDaoException e) {
      throw new RuntimeException(e);
    }
      
    new Thread(){
      public void run(){
        while (goOn){
          try {
            if (workerThreads.size() < maxWorkers) {
              List<String> children = WorkerDao.finalAllPlanNames(zk);  
              logger.debug("List of plans: " + children);
              for (String child: children){
                considerStarting(child);
              }
            } else {
              logger.debug("Will not attempt to start worker. Already at max workers " + workerThreads.size());
            }
          } catch (Exception ex){
            logger.warn("Exception during scan", ex);
          }
          try {
            Thread.sleep(rescanMillis);
          } catch (InterruptedException e) {
            logger.warn(e);
          }
        }
      }
    }.start();
  }

  @VisibleForTesting
  public void applyPlan(Plan plan){
    try {
      WorkerDao.createOrUpdatePlan(plan, zk);
    } catch (WorkerDaoException e) {
      logger.warn("Failed writing/updating plan", e);
    }
  }
  
  @VisibleForTesting
  public void deletePlan(Plan plan){
    try {
      WorkerDao.deletePlan(zk, plan);
    } catch (WorkerDaoException e) {
      logger.warn("Failed deleting/updating plan", e);
    }
  }
  
  /**
   * Determines if the plan can be run. IE not disabled and not
   * malformed
   * @return true if the plan seems reasonable enough to run
   */
  public boolean isPlanSane(Plan plan){
    if (plan == null){
      logger.warn("did not find plan");
      return false;
    }
    if (plan.isDisabled()){
      logger.debug("disabled "+ plan.getName());
      return false;
    }
    if (plan.getFeedDesc() == null){
      logger.warn("feed was null "+ plan.getName());
      return false;
    }
    return true;
  }

  @VisibleForTesting
  boolean alreadyAtMaxWorkersPerNode(Plan plan, List<String> workerUuids, List<Worker> workingOnPlan){
    if (plan.getMaxWorkersPerNode() == 0){
      return false;
    }
    int numberOfWorkersRunningInDaemon = 0;
    if (workingOnPlan == null){
      return false;
    }
    for (Worker worker: workingOnPlan){
      for (String uuid : workerUuids ) {
        if (worker.getMyId().toString().equals(uuid)){
          numberOfWorkersRunningInDaemon++;
        }
      }
    }
    if (numberOfWorkersRunningInDaemon >= plan.getMaxWorkersPerNode()){
      return true;
    } else {
      return false;
    }
  }
  
  private void considerStarting(String child){
    Plan plan = null;
    List<String> workerUuidsWorkingOnPlan = null;
    try {
      plan = WorkerDao.findPlanByName(zk, child);
      if (!child.equals(plan.getName())){
        logger.warn(String.format("Node name %s is not the same is the json value %s will not start", child, plan.getName()));
        return;
      }
      workerUuidsWorkingOnPlan = WorkerDao.findWorkersWorkingOnPlan(zk, plan);
    } catch (WorkerDaoException e) {
      logger.warn("Problem finding plan or workers for plan ", e);
      return;
    }
    if (alreadyAtMaxWorkersPerNode(plan, workerUuidsWorkingOnPlan, workerThreads.get(plan))){
      return;
    }
    if (!isPlanSane(plan)){
      return;
    } 
    logger.debug("trying to acqure lock on " + WorkerDao.LOCKS_ZK + "/" + plan.getName());
    try {
      WorkerDao.maybeCreatePlanLockDir(zk, plan);
    } catch (WorkerDaoException e1) {
      logger.warn(e1);
      return;
    }
    final CountDownLatch c = new CountDownLatch(1);
    WriteLock l = new WriteLock(zk, WorkerDao.LOCKS_ZK + "/" + plan.getName(), null);
    l.setLockListener(new LockListener(){

      @Override
      public void lockAcquired() {
        logger.debug(myId + " counting down");
        c.countDown();
      }

      @Override
      public void lockReleased() {
        logger.debug(myId + " released");
      }
      
    });
    try {
      boolean gotLock = l.lock(); 
      /*
      if (!gotLock){
        logger.debug("did not get lock");
        return;
      }*/
      boolean hasLatch = c.await(3000, TimeUnit.MILLISECONDS);
      if (hasLatch){
        /* plan could have been disabled after latch:Maybe editing the plan should lock it as well */
        try {
          plan = WorkerDao.findPlanByName(zk, child);
        } catch (WorkerDaoException e) {
          logger.warn(e);
          return;
        }
        if (plan.isDisabled()){
          logger.debug("disabled "+ plan.getName());
          return;
        } 
        List<String> workerUuids = WorkerDao.findWorkersWorkingOnPlan(zk, plan);
        if (workerUuids.size() >= plan.getMaxWorkers()) {
          logger.debug("already running max children:" + workerUuids.size() + " planmax:"
                  + plan.getMaxWorkers() + " running:" + workerUuids);
          return;
        } 
        logger.debug("starting worker");
        try {
          Worker worker = new Worker(plan, workerUuids, this);
          worker.init();
          worker.start();
          addWorkerToList(plan, worker);
        } catch (RuntimeException e){
          throw new WorkerStartException(e);
        }    
      }
    } catch (KeeperException | InterruptedException | WorkerDaoException | WorkerStartException e) {
      logger.warn("getting lock", e); 
    } finally {
      try {
        l.unlock();
      } catch (RuntimeException ex){
        logger.warn("Unable to unlock ", ex);
      }
    }
  }
  
  private void addWorkerToList(Plan plan, Worker worker) {
    logger.debug("adding worker " + worker.getMyId() + " to plan "+plan.getName());
    List<Worker> list = workerThreads.get(plan);
    if (list == null) {
      list = Collections.synchronizedList(new ArrayList<Worker>());
    }
    list.add(worker);
    workerThreads.put(plan, list);
  }


  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected){
      awaitConnection.countDown();
    } 
  }

  public String getMyId() {
    return myId;
  }

  public void setMyId(String myId) {
    this.myId = myId;
  }
 
  public void stop(){
    this.goOn = false;
  }

  public Properties getProperties() {
    return properties;
  }

  public long getRescanMillis() {
    return rescanMillis;
  }

  public void setRescanMillis(long rescanMillis) {
    this.rescanMillis = rescanMillis;
  }
  
  public String getHostname() {
    return hostname;
  }

  @VisibleForTesting
  void setHostname(String hostname) {
    this.hostname = hostname;
  }
  
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public static void main (String [] args){
    TeknekDaemon td = new TeknekDaemon(System.getProperties());
    td.init();
  }  
}
