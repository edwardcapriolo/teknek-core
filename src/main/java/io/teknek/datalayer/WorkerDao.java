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
package io.teknek.datalayer;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.daemon.WorkerStatus;
import io.teknek.plan.Bundle;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This component deals with persistence into zk for the worker node
 * @author edward
 *
 */
public class WorkerDao {
  
  public final static String ENCODING = "UTF-8";
  
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  private final static Logger LOGGER = Logger.getLogger(WorkerDao.class.getName());
    
  /**
   * Base directory of the entire application
   */
  public final String BASE_ZK;
  /**
   * ephemeral nodes for worker registration live here
   */
  public final String WORKERS_ZK;
  /**
   * plans of stuff for workers to do live here
   */
  public final String PLANS_ZK;
  /**
   * saved feeds and operators
   */
  public final String SAVED_ZK;
  /**
   * Holds zk locks for choosing plans
   */
  public final String LOCKS_ZK;
  
  public WorkerDao(){
    BASE_ZK = "/teknek";
    WORKERS_ZK = BASE_ZK + "/workers";
    PLANS_ZK = BASE_ZK + "/plans";
    SAVED_ZK = BASE_ZK + "/saved";
    LOCKS_ZK = BASE_ZK + "/locks";
  }
  
  public WorkerDao(String base){
    BASE_ZK = base;
    WORKERS_ZK = BASE_ZK + "/workers";
    PLANS_ZK = BASE_ZK + "/plans";
    SAVED_ZK = BASE_ZK + "/saved";
    LOCKS_ZK = BASE_ZK + "/locks";
  }
  
  /**
   * Creates all the required base directories in ZK for the application to run 
   * @param zk
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void createZookeeperBase(ZooKeeper zk) throws WorkerDaoException {
    try {
      if (zk.exists(BASE_ZK, true) == null) {
        LOGGER.info("Creating " + BASE_ZK + " heirarchy");
        try {
          zk.create(BASE_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e){ }
      }
      if (zk.exists(WORKERS_ZK, false) == null) {
        try { 
          zk.create(WORKERS_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e){ }
      }
      if (zk.exists(PLANS_ZK, true) == null) {
        try {
          zk.create(PLANS_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e){ }
      }
      if (zk.exists(SAVED_ZK, false) == null) {
        try { 
          zk.create(SAVED_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e){ }
      }
      if (zk.exists(LOCKS_ZK, false) == null) {
        try { 
          zk.create(LOCKS_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e){ }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  /**
   * Returns the node name of all the ephemeral nodes under a plan. This is effectively who is
   * working on the plan.
   * @param zk
   * @param plan
   * @return 
   * @throws WorkerDaoException If there are zookeeper problems
   */
  public List<String> findWorkersWorkingOnPlan(ZooKeeper zk, Plan plan) throws WorkerDaoException{
    try {
      return zk.getChildren(PLANS_ZK + "/" + plan.getName(), false);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  /**
   * 
   * @param zk
   * @return a list of all plans stored in zk
   * @throws KeeperException
   * @throws InterruptedException
   */
  public List<String> finalAllPlanNames (ZooKeeper zk) throws WorkerDaoException {
    try {
      return zk.getChildren(PLANS_ZK, false);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  /**
   * Search zookeeper for a plan with given name.
   * 
   * @param zk
   * @param name
   * @return null if plan not found, null if plan is corrupt data
   * @throws WorkerDaoException
   *           for zookeeper problems
   */
  public Plan findPlanByName(ZooKeeper zk, String name) throws WorkerDaoException {
    Stat planStat;
    byte[] planBytes;
    try {
      planStat = zk.exists(PLANS_ZK + "/" + name, false);
      if (planStat == null) {
        return null;
      }
      planBytes = zk.getData(PLANS_ZK + "/" + name, false, planStat);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
    Plan p = null;
    try {
      p = deserializePlan(planBytes);
    } catch (JsonParseException | JsonMappingException e) {
      LOGGER.warn("while parsing plan " + name, e);
    } catch (IOException e) {
      LOGGER.warn("while parsing plan " + name, e);
    }
    return p;
  }

  public static Plan deserializePlan(byte[] b) throws JsonParseException, JsonMappingException,
          IOException {
    return MAPPER.readValue(b, Plan.class);
  }
  
  public static byte[] serializePlan(Plan plan) throws WorkerDaoException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      MAPPER.writeValue(baos, plan);
    } catch (IOException ex) {
      throw new WorkerDaoException(ex);
    }
    return baos.toByteArray();
  }
  
  /**
   * Creates or updates a plan in zookeeper.
   * @param plan
   * @param zk
   * @throws WorkerDaoException if malformed plan or communication error with zookeeper
   */
  public void createOrUpdatePlan(Plan plan, ZooKeeper zk) throws WorkerDaoException {
      try {
        createZookeeperBase(zk);
        Stat s = zk.exists(PLANS_ZK+ "/" + plan.getName(), false);
        if (s != null) {
          zk.setData(PLANS_ZK+ "/" + plan.getName(), serializePlan(plan), s.getVersion());
        } else {
          zk.create(PLANS_ZK+ "/" + plan.getName(), serializePlan(plan), Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);
        }
      } catch (KeeperException | InterruptedException e) {
        throw new WorkerDaoException(e);
      }
  }
  
  /**
   * Creates an ephemeral node so that we can determine how many current live nodes there are
   * @param zk
   * @param d
   * @throws WorkerDaoException
   */
  public void createEphemeralNodeForDaemon(ZooKeeper zk, TeknekDaemon d) throws WorkerDaoException {
    try {
      byte [] hostbytes = d.getHostname().getBytes(ENCODING);
      zk.create(WORKERS_ZK + "/" + d.getMyId(), hostbytes , Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public List<String> findAllWorkers(ZooKeeper zk) throws WorkerDaoException {
    try {
      List<String> found = zk.getChildren(WORKERS_ZK, false);
      if (found == null){
        return Arrays.asList();
      } else {
        return found;
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  /**
   * Gets the status of each worker. The status contains the partitionId being consumed. This information
   * helps the next worker bind to an unconsumed partition
   * @param zk
   * @param plan
   * @param otherWorkers
   * @return
   * @throws WorkerDaoException
   */
  public List<WorkerStatus> findAllWorkerStatusForPlan(ZooKeeper zk, Plan plan, List<String> otherWorkers) throws WorkerDaoException{
    List<WorkerStatus> results = new ArrayList<WorkerStatus>();
    for (String worker : otherWorkers) {
      String lookAtPath = PLANS_ZK + "/" + plan.getName() + "/" + worker;
      try {
        Stat stat = zk.exists(lookAtPath, false);
        byte[] data = zk.getData(lookAtPath, false, stat);
        results.add(MAPPER.readValue(data, WorkerStatus.class));
      } catch (KeeperException | InterruptedException | IOException e) {
        throw new WorkerDaoException(e);
      }
    }
    return results;
  }
  
  /**
   * Registers an ephemeral node representing ownership of a feed partition
   * @param zk
   * @param plan
   * @param s
   * @throws WorkerDaoException 
   */
  public void registerWorkerStatus(ZooKeeper zk, Plan plan, WorkerStatus s) throws WorkerDaoException{
    String writeToPath = PLANS_ZK + "/" + plan.getName() + "/" + s.getWorkerUuid();
    try {
      zk.create(writeToPath, MAPPER.writeValueAsBytes(s), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOGGER.debug("Registered as ephemeral " + writeToPath);
      zk.exists(PLANS_ZK+ "/" + plan.getName(), true);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public FeedDesc loadSavedFeedDesc(ZooKeeper zk, String group, String name) throws WorkerDaoException {
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "feedDesc";
    try {
      Stat stat = zk.exists(readPath, false);
      if (stat != null){
        byte [] data = zk.getData(readPath, false, stat);
        return deserializeFeedDesc(data);
      } else {
        throw new WorkerDaoException("not found in zk");
      }
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public OperatorDesc loadSavedOperatorDesc(ZooKeeper zk, String group, String name) throws WorkerDaoException{
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "operatorDesc";
    try {
      Stat stat = zk.exists(readPath, false);
      if (stat != null){
        byte [] data = zk.getData(readPath, false, stat);
        return deserializeOperatorDesc(data);
      } else {
        throw new WorkerDaoException("not found in zk");
      }
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public void saveOperatorDesc(ZooKeeper zk, OperatorDesc desc, String group, String name)
          throws WorkerDaoException {
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "operatorDesc";
    createZookeeperBase(zk);
    try {
      String pathCreated = zk.create(readPath, serializeOperatorDesc(desc), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
      LOGGER.debug("Created " + pathCreated);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }

  public void saveFeedDesc(ZooKeeper zk, FeedDesc desc, String group, String name)
          throws WorkerDaoException {
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "feedDesc";
    createZookeeperBase(zk);
    try {
      String pathCreated = zk.create(readPath, serializeFeedDesc(desc), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
      LOGGER.debug("Created " + pathCreated);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static OperatorDesc deserializeOperatorDesc(byte [] b) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    OperatorDesc p1 = om.readValue(b, OperatorDesc.class);
    return p1;
  }
  
  public static FeedDesc deserializeFeedDesc(byte [] b) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    FeedDesc p1 = om.readValue(b, FeedDesc.class);
    return p1;
  }
  
  public static byte [] serializeFeedDesc(FeedDesc desc) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    byte [] b = om.writeValueAsBytes(desc);
    return b;
  }
  
  public static byte [] serializeOperatorDesc(OperatorDesc desc) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    byte [] b = om.writeValueAsBytes(desc);
    return b;
  }


  public static Bundle getBundleFromUrl(URL u) throws WorkerDaoException {
    Bundle b = null;
    InputStream is = null;
    try {
      URLConnection yc = u.openConnection();
      ObjectMapper om = new ObjectMapper();
      is = yc.getInputStream();
      b = om.readValue(is, Bundle.class);
    } catch (IOException e) {
      LOGGER.warn(e.getMessage());
      throw new WorkerDaoException(e);
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOGGER.debug(e);
        }
      }
    }
    return b;
  }

  public void saveBundle(ZooKeeper zk, Bundle b) throws WorkerDaoException {
    for (OperatorDesc o : b.getOperatorList() ){
      saveOperatorDesc(zk, o, b.getPackageName(), o.getName());
    }
    for (FeedDesc f: b.getFeedDescList()){
      saveFeedDesc(zk, f, b.getPackageName(), f.getName());
    }
  }
  
  /**
   * Note you should call stop the plan if it is running before deleting 
   * @param zk
   * @param p
   * @throws WorkerDaoException
   */
  public void deletePlan(ZooKeeper zk, Plan p) throws WorkerDaoException {
    String planNode = PLANS_ZK + "/" + p.getName();
    try {
      Stat s = zk.exists(planNode, false);
      if (s != null) {
        zk.delete(planNode, s.getVersion());
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public void maybeCreatePlanLockDir(ZooKeeper zk, Plan plan) throws WorkerDaoException {
    try {
      String planLock = LOCKS_ZK + "/" + plan.getName();
      if (zk.exists(planLock, false) == null) {
        LOGGER.debug("Creating " + planLock);
        zk.create(planLock, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
}
