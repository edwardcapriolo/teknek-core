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
package io.teknek.driver;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;

import io.teknek.collector.CollectorProcessor;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;

/** driver consumes data from a feed partition and inserts it into operators */
public class Driver implements Runnable {
  private FeedPartition fp;
  private DriverNode driverNode;
  private volatile boolean goOn;
  private long tuplesSeen;
  private OffsetStorage offsetStorage;
  /**
   * after how many tuples should the offset be committed. 0 disables offsetCommits
   */
  private int offsetCommitInterval;
  
  private final Meter dequedByPlan;
  private final Histogram timeToDequeue;
  private final Meter processedByPlan;
  private final Meter retriesByPlan;
  private final Histogram timeToProcess;

  /**
   * 
   * @param fp feed partition to consume from
   * @param operator root operator of the driver
   * @param offsetStorage can be null if user does not wish to have offset storage
   */
  public Driver(FeedPartition fp, Operator operator, OffsetStorage offsetStorage, 
          CollectorProcessor collectorProcessor, int offsetCommitInterval, MetricRegistry metricRegistry, String planName){
    this.driverNode = new DriverNode(operator, collectorProcessor);
    this.goOn = true;
    this.fp = fp;
    this.offsetStorage = offsetStorage;
    this.offsetCommitInterval = offsetCommitInterval;
    this.dequedByPlan = metricRegistry.meter(planName + ".driver." + "deque");
    this.timeToDequeue = metricRegistry.histogram(planName + ".driver." + "dequeue_time_nanos");
    this.processedByPlan = metricRegistry.meter(planName + ".driver." + "processed");
    this.retriesByPlan = metricRegistry.meter(planName + ".driver." + "retries");
    this.timeToProcess = metricRegistry.histogram(planName + ".driver." + "process_time_nanos");
  }
  
  public void initialize(){
    driverNode.initialize();
    fp.initialize();
  }
  
  public void run(){
    boolean hasNext = false;
    do {
      if (!goOn){
        break;
      }
      ITuple t = new Tuple();
      long start = System.nanoTime();
      hasNext = fp.next(t);
      timeToDequeue.update(System.nanoTime() - start);
      tuplesSeen++;
      dequedByPlan.mark(1);
      int attempts = 0;
      long processStart = System.nanoTime();
      while (attempts++ < driverNode.getCollectorProcessor().getTupleRetry() + 1) {
        try {
          driverNode.getOperator().handleTuple(t);
          processedByPlan.mark();
          break;
        } catch (RuntimeException ex) {
          retriesByPlan.mark();
        }
      }
      timeToProcess.update(System.nanoTime() - processStart);
      maybeDoOffset();
      if (!hasNext) {
        break;
      }
    } while (goOn);
    gracefulEnd();
  }
  
  private void gracefulEnd(){
    fp.close();
    if (offsetStorage != null && fp.supportsOffsetManagement()){
      doOffsetInternal();
    }
    closeTopology();
  }
  /**
   * To do offset storage we let the topology drain itself out. Then we commit. 
   */
  public void maybeDoOffset(){
    long seen = tuplesSeen;
    if (offsetCommitInterval > 0 && seen % offsetCommitInterval == 0 && offsetStorage != null && fp.supportsOffsetManagement()){
      doOffsetInternal();
    }
  }
  
  @VisibleForTesting
  void doOffsetInternal(){
    drainTopology();
    Offset offset = offsetStorage.getCurrentOffset(); 
    offsetStorage.persistOffset(offset);
  }
  
  public DriverNode getDriverNode() {
    return driverNode;
  }

  public void setDriverNode(DriverNode driverNode) {
    this.driverNode = driverNode;
  }
  
  public void drainTopology(){
    DriverNode root = driverNode;
    drainTopologyInternal(root); 
  }
  
  private void drainTopologyInternal(DriverNode node){
    while (node.getCollectorProcessor().getCollector().size() > 0){
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
      }
    }
    node.getOperator().commit();
    for (DriverNode child: node.getChildren()){
      drainTopologyInternal(child);
    }
  }
  
  public void closeTopology(){
    DriverNode root = driverNode;
    closeTopologyInternal(root);
  }
  
  
  private void closeTopologyInternal(DriverNode node){
    node.getOperator().close();
    for (DriverNode child: node.getChildren()){
      closeTopologyInternal(child);
    }
  }
  
  
  public String toString(){
    StringBuilder sb  = new StringBuilder();
    sb.append("Feed Partition " + fp.getPartitionId() + " ");
    sb.append("driver node " + this.driverNode.getClass().getName());
    return sb.toString();
  }
  
  public void prettyPrint(){
    System.out.println("+++++++");
    System.out.println("Feed Partition " + fp.getFeed().getClass() + " " );
    System.out.println("Feed Partition " + fp.getPartitionId() + " " );
    System.out.println("-------");
    System.out.println("--"+driverNode.getOperator().getClass().getName());
    for (DriverNode child: driverNode.getChildren() ){
      child.prettyPrint(2);
    }
    System.out.println("+++++++");
  }

  public boolean getGoOn() {
    return goOn;
  }

  public void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }
  
}