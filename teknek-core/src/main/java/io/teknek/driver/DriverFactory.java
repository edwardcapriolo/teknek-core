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
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import io.teknek.collector.CollectorProcessor;
import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.GroovyOperator;
import io.teknek.model.Operator;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OffsetStorageDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class DriverFactory {

  public static Driver createDriver(FeedPartition feedPartition, Plan plan){
    OperatorDesc desc = plan.getRootOperator();
    Operator oper = buildOperator(desc);
    OffsetStorage offsetStorage = null;
    OffsetStorageDesc offsetDesc = plan.getOffsetStorageDesc();
    if (offsetDesc != null && feedPartition.supportsOffsetManagement()){
      offsetStorage = buildOffsetStorage(feedPartition, plan, offsetDesc);
      Offset offset = offsetStorage.findLatestPersistedOffset();
      if (offset != null){
        feedPartition.setOffset(new String(offset.serialize()));
      }
    }
    CollectorProcessor cp = new CollectorProcessor();
    cp.setTupleRetry(plan.getTupleRetry());
    int offsetCommitInterval = plan.getOffsetCommitInterval();
    if (offsetCommitInterval == 0){
      offsetCommitInterval = 10;
    }
    Driver driver = new Driver(feedPartition, oper, offsetStorage, cp, offsetCommitInterval);
    DriverNode root = driver.getDriverNode();
    
    recurseOperatorAndDriverNode(desc, root);
    return driver;
  }
  
  private static void recurseOperatorAndDriverNode(OperatorDesc desc, DriverNode node){
    List<OperatorDesc> children = desc.getChildren();
    for (OperatorDesc childDesc: children){
      Operator oper = buildOperator(childDesc);
      CollectorProcessor cp = new CollectorProcessor();
      cp.setTupleRetry(node.getCollectorProcessor().getTupleRetry());
      DriverNode childNode = new DriverNode(oper, cp);
      node.addChild(childNode);
      recurseOperatorAndDriverNode(childDesc, childNode);
    }
  }
  
  public static Operator buildOperator(OperatorDesc operatorDesc){
    Operator operator = null;
    if (operatorDesc.getSpec() == null){
      try {
        operator = (Operator) Class.forName(operatorDesc.getTheClass()).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else if (operatorDesc.getSpec().equals("groovy")){
      try (GroovyClassLoader gc = new GroovyClassLoader()){
        Class<?> c = gc.parseClass( operatorDesc.getScript()) ;
        operator = (Operator) c.newInstance();
      } catch (InstantiationException | IllegalAccessException | IOException e) {
        throw new RuntimeException (e);
      }
    } else if (operatorDesc.getSpec().equals("groovyclosure")){
      GroovyShell shell = new GroovyShell();
      Object result = shell.evaluate(operatorDesc.getScript());
      if (result instanceof Closure){
        return new GroovyOperator((Closure) result);
      } else {
        throw new RuntimeException("result was wrong type "+ result);
      }
    } else {
      throw new RuntimeException(operatorDesc.getSpec() +" dont know how to handle that");
    }
    return operator;
  }
  
  public static OffsetStorage buildOffsetStorage(FeedPartition feedPartition, Plan plan, OffsetStorageDesc offsetDesc){
    OffsetStorage offsetStorage = null;
    Class [] paramTypes = new Class [] { FeedPartition.class, Plan.class, Map.class };    
    Constructor<OffsetStorage> offsetCons = null;
    try {
      offsetCons = (Constructor<OffsetStorage>) Class.forName(offsetDesc.getOperatorClass()).getConstructor(
              paramTypes);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try {
      offsetStorage = offsetCons.newInstance(feedPartition, plan, offsetDesc.getParameters());
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    return offsetStorage;
  }
  
  /**
   * Build a feed using reflection
   * @param feedDesc
   * @return
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Feed buildFeed(FeedDesc feedDesc){
    Feed feed = null;
    Class [] paramTypes = new Class [] { Map.class };    
    Constructor<Feed> feedCons = null;
    try {
      feedCons = (Constructor<Feed>) Class.forName(feedDesc.getTheClass()).getConstructor(
              paramTypes);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try {
      feed = feedCons.newInstance(feedDesc.getProperties());
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    return feed;
  }
  
}
