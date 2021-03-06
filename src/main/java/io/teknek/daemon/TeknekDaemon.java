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
import io.teknek.graphite.reporter.CommonGraphiteReporter;
import io.teknek.graphite.reporter.SimpleJmxReporter;
import io.teknek.plan.Plan;
import io.teknek.zookeeper.RestablishingKeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;

public class TeknekDaemon {

  private final static Logger logger = Logger.getLogger(TeknekDaemon.class.getName());
  public static final String ZK_SERVER_LIST = "teknek.zk.servers";
  public static final String MAX_WORKERS = "teknek.max.workers";
  public static final String DAEMON_ID = "teknek.daemon.id";
  public static final String ZK_BASE_DIR = "zk.base.dir";
  
  public static final String GRAPHITE_HOST = "teknek.graphite.host";
  public static final String GRAPHITE_PORT = "teknek.graphite.port";
  public static final String GRAPHITE_CLUSTER = "teknek.graphite.cluster";
  
  private int maxWorkers = 4;
  private String myId;
  private Properties properties;
  private long rescanMillis = 5000;
  ConcurrentHashMap<Plan, List<Worker>> workerThreads;
  private boolean goOn = true;
  private String hostname;
  private MetricRegistry metricRegistry;
  private SimpleJmxReporter jmxReporter;
  private CommonGraphiteReporter graphiteReporter;
  private RestablishingKeeper reKeeper;
  private WorkerDao workerDao;
  
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
    final TeknekDaemon t = this;
    reKeeper = new RestablishingKeeper(properties.getProperty(ZK_SERVER_LIST)) {
      @Override
      public void onReconnect(ZooKeeper zooKeeper, CuratorFramework framework) {
        try {
          workerDao.createZookeeperBase();
          workerDao.createEphemeralNodeForDaemon(t);
        } catch (WorkerDaoException e) {
          throw new RuntimeException(e);
        }
      }
    };
    if (properties.containsKey(ZK_BASE_DIR)){
      workerDao = new WorkerDao(properties.getProperty(ZK_BASE_DIR), reKeeper);
    } else {
      workerDao = new WorkerDao(reKeeper);
    }
    metricRegistry = new MetricRegistry();
    jmxReporter = new SimpleJmxReporter(metricRegistry, "teknek-core");
  }
  
  public void init() throws InterruptedException {
    jmxReporter.init();
    if (properties.get(GRAPHITE_HOST) != null){
      graphiteReporter = new CommonGraphiteReporter(metricRegistry, 
              properties.getProperty(GRAPHITE_HOST), 
              Integer.parseInt(properties.getProperty(GRAPHITE_PORT)), true);
      graphiteReporter.setClusterName(properties.getProperty(GRAPHITE_CLUSTER));
      graphiteReporter.init();
    }
    logger.info("Daemon id:" + myId + " Connecting to:" + properties.getProperty(ZK_SERVER_LIST));
    reKeeper.init();
      
  }
  
  @VisibleForTesting
  public void runOnce(){
    try {
      List<String> children = workerDao.finalAllPlanNames();
      logger.debug("List of plans: " + children);
      for (String child: children){
        int currentlyWorking = currentlyWorking();
        if (currentlyWorking >= maxWorkers) {
          logger.debug("Will not attempt to start worker. Already at max workers " + currentlyWorking);
          return;
        }
        considerStarting(child);
      }
    } catch (WorkerDaoException e) {
      logger.warn(e);
    }  
  }
  
  private int currentlyWorking(){
    if (workerThreads == null){
      return 0;
    }
    int total = 0;
    for (Entry<Plan, List<Worker>> entry : workerThreads.entrySet()){
      if (entry.getValue() != null){
        total += entry.getValue().size();
      }
    }
    return total;
  }
  
  public void start(){
    new Thread(){
      public void run(){
        while (goOn){
          runOnce();
          try {
            Thread.sleep(rescanMillis);
          } catch (InterruptedException e) { }
        }
      }
    }.start();
  }

  @VisibleForTesting
  public void applyPlan(Plan plan){
    try {
      workerDao.createOrUpdatePlan(plan);
    } catch (WorkerDaoException e) {
      logger.warn("Failed writing/updating plan", e);
    }
  }
  
  @VisibleForTesting
  public List<String> findAllWorkers(){
    try {
      return workerDao.findAllWorkers();
    } catch (WorkerDaoException e) {
      logger.warn(e);
    }
    return null;
  }
  
  @VisibleForTesting
  public void deletePlan(Plan plan){
    try {
      workerDao.deletePlan(plan);
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
  
  private void considerStarting(String child) throws WorkerDaoException {
    Plan plan = workerDao.findPlanByName(child);
    if (plan == null){
      logger.warn(String.format("Did not find a valid plan under node name %s", child));
      return;
    }
    if (!child.equals(plan.getName())){
      logger.warn(String.format("Node name %s is not the same is the json value %s will not start", child, plan.getName()));
      return;
    }
    List<String> workerUuidsWorkingOnPlan = workerDao.findWorkersWorkingOnPlan(plan);
    if (alreadyAtMaxWorkersPerNode(plan, workerUuidsWorkingOnPlan, workerThreads.get(plan))){
      return;
    }
    if (!isPlanSane(plan)){
      return;
    } 
    logger.debug("trying to acqure lock on " + workerDao.LOCKS_ZK + "/" + plan.getName());
    workerDao.maybeCreatePlanLockDir(plan);
    InterProcessMutex lock = new InterProcessMutex(reKeeper.getCuratorFramework(), workerDao.LOCKS_ZK + "/" + plan.getName());
    try {
      boolean gotLock = false;
      try {
        gotLock = lock.acquire(3, TimeUnit.SECONDS);
      } catch (Exception e) {
        logger.warn("could not aquire lock", e);
      }
      if (!gotLock){
        logger.debug("did not get lock");
        return;
      }
      plan = workerDao.findPlanByName(child);
      if (plan.isDisabled()){
        logger.debug("disabled "+ plan.getName());
        return;
      } 
      List<String> workerUuids = workerDao.findWorkersWorkingOnPlan(plan);
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
      
    } catch (WorkerDaoException | WorkerStartException e) {
      logger.warn("getting lock", e); 
    } finally {
      try {
        lock.release();
      } catch (Exception e) {
        logger.warn("Problems releasing lock", e);
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

  public WorkerDao getWorkerDao() {
    return workerDao;
  }
  
  public RestablishingKeeper getReestablishingKeeper(){
    return this.reKeeper;
  }

  public static void main (String [] args) throws InterruptedException {
    TeknekDaemon td = new TeknekDaemon(System.getProperties());
    td.init();
    td.start();
  }  
}
