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


import groovy.lang.Closure;
import io.teknek.collector.CollectorProcessor;
import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.GroovyOperator;
import io.teknek.model.Operator;
import io.teknek.nit.NitDesc;
import io.teknek.nit.NitException;
import io.teknek.nit.NitFactory;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;
import io.teknek.plan.DynamicInstantiatable;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OffsetStorageDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;

/**
 * Builds instances of Driver for a given plan.
 * @author edward
 *
 */
public class DriverFactory {

  final static Logger logger = Logger.getLogger(DriverFactory.class.getName());
  
  /**
   * Given a FeedParition and Plan create a Driver that will consume from the feed partition
   * and execute the plan.
   * @param feedPartition
   * @param plan
   * @return an uninitialized Driver
   */
  public static Driver createDriver(FeedPartition feedPartition, Plan plan, MetricRegistry metricRegistry){
    populateFeedMetricInfo(plan, feedPartition, metricRegistry);
    OperatorDesc desc = plan.getRootOperator();
    Operator oper = buildOperator(desc, metricRegistry, plan.getName(), feedPartition);
    OffsetStorage offsetStorage = null;
    OffsetStorageDesc offsetDesc = plan.getOffsetStorageDesc();
    if (offsetDesc != null && feedPartition.supportsOffsetManagement()){
      offsetStorage = buildOffsetStorage(feedPartition, plan, offsetDesc);
      Offset offset = offsetStorage.findLatestPersistedOffset();
      if (offset != null){
        feedPartition.setOffset(new String(offset.serialize(), Charsets.UTF_8));
      }
    }
    CollectorProcessor cp = new CollectorProcessor();
    cp.setTupleRetry(plan.getTupleRetry());
    int offsetCommitInterval = plan.getOffsetCommitInterval();
    Driver driver = new Driver(feedPartition, oper, offsetStorage, cp, offsetCommitInterval, metricRegistry, plan.getName());
    recurseOperatorAndDriverNode(desc, driver.getDriverNode(), metricRegistry, feedPartition);
    return driver;
  }
  
  private static void recurseOperatorAndDriverNode(OperatorDesc desc, DriverNode node, 
          MetricRegistry metricRegistry, FeedPartition feedPartition){
    List<OperatorDesc> children = desc.getChildren();
    for (OperatorDesc childDesc: children){
      Operator oper = buildOperator(childDesc, metricRegistry, node.getOperator().getPath(), feedPartition);
      CollectorProcessor cp = new CollectorProcessor();
      cp.setTupleRetry(node.getCollectorProcessor().getTupleRetry());
      DriverNode childNode = new DriverNode(oper, cp);
      node.addChild(childNode);
      recurseOperatorAndDriverNode(childDesc, childNode, metricRegistry, feedPartition);
    }
  }
  
  /**
   * DynamicInstantiatable has some pre-nit strings it uses as for spec
   * that do not match nit-compiler. Here we correct these and return a new
   * object
   * 
   * @param d
   * @return the 
   */
  private static NitDesc nitDescFromDynamic(DynamicInstantiatable d){
    NitDesc nd = new NitDesc();
    nd.setScript(d.getScript());
    nd.setTheClass(d.getTheClass());
    if (d.getSpec() == null || "java".equals(d.getSpec())){
      nd.setSpec(NitDesc.NitSpec.JAVA_LOCAL_CLASSPATH);
    } else if ("groovy".equals(d.getSpec())){
      nd.setSpec(NitDesc.NitSpec.GROOVY_CLASS_LOADER);
    } else if ("groovyclosure".equals(d.getSpec())){
      nd.setSpec(NitDesc.NitSpec.GROOVY_CLOSURE);
    } else if ("url".equals(d.getSpec())){
      nd.setSpec(NitDesc.NitSpec.JAVA_URL_CLASSLOADER);
    } else {
      nd.setSpec(NitDesc.NitSpec.valueOf(d.getSpec()));
    }
    return nd;
  }

  /**
   * OperatorDesc can describe local reasources, URL, loaded resources and dynamic resources like
   * groovy code. This method instantiates an Operator based on the OperatorDesc.
   * 
   * @param operatorDesc
   * @return
   */
  public static Operator buildOperator(OperatorDesc operatorDesc, MetricRegistry metricRegistry, String planPath, FeedPartition feedPartition) {
    Operator operator = null;
    NitFactory nitFactory = new NitFactory();
    NitDesc nitDesc = nitDescFromDynamic(operatorDesc);
    try {
      if (nitDesc.getSpec() == NitDesc.NitSpec.GROOVY_CLOSURE){
        operator = new GroovyOperator((Closure) nitFactory.construct(nitDesc));
      } else {
        operator = nitFactory.construct(nitDesc);
      }
    } catch (NitException e) {
      throw new RuntimeException(e);
    }
    operator.setProperties(operatorDesc.getParameters());
    operator.setMetricRegistry(metricRegistry);
    operator.setPartitionId(feedPartition.getPartitionId());
    String myName = operatorDesc.getName();
    if (myName == null){
      myName = operatorDesc.getTheClass();
      if (myName.indexOf(".") > -1){
        String[] parts = myName.split("\\.");
        myName = parts[parts.length-1];
      }
    }
    operator.setPath(planPath + "." + myName);
    return operator;
  }
  
  public static OffsetStorage buildOffsetStorage(FeedPartition feedPartition, Plan plan, OffsetStorageDesc offsetDesc){
    OffsetStorage offsetStorage = null;
    Class [] paramTypes = new Class [] { FeedPartition.class, Plan.class, Map.class };
    NitFactory nit = new NitFactory();
    NitDesc desc = new NitDesc();
    desc.setSpec(NitDesc.NitSpec.JAVA_LOCAL_CLASSPATH);
    desc.setConstructorParameters(paramTypes);
    desc.setConstructorArguments( new Object [] { feedPartition, plan, offsetDesc.getParameters() });
    try {
      offsetStorage = nit.construct(desc);
    } catch (NitException e) {
      throw new RuntimeException(e);
    }
    return offsetStorage;
  }
  
  /**
   * Build a feed using reflection
   * @param feedDesc
   * @return
   */
  public static Feed buildFeed(FeedDesc feedDesc){
    Feed feed = null;
    NitFactory nitFactory = new NitFactory();
    NitDesc nitDesc = nitDescFromDynamic(feedDesc);
    nitDesc.setConstructorParameters(new Class [] { Map.class });
    nitDesc.setConstructorArguments(new Object[] { feedDesc.getProperties() });
    try {
      feed = nitFactory.construct(nitDesc);
    } catch (NitException e) {
      throw new RuntimeException(e);
    }
    return feed;
  }
  
  private static void populateFeedMetricInfo(Plan p, FeedPartition fp, MetricRegistry r){
    String myName = p.getFeedDesc().getName();
    if (myName == null){
      myName = p.getFeedDesc().getTheClass();
      if (myName.indexOf(".") > -1){
        String[] parts = myName.split("\\.");
        myName = parts[parts.length-1];
      }
    }
    fp.setPath(p.getName() + "." + myName);
    fp.setMetricRegistry(r);
  }
}
