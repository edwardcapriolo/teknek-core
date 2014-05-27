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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

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
  public static Driver createDriver(FeedPartition feedPartition, Plan plan){
    OperatorDesc desc = plan.getRootOperator();
    Operator oper = buildOperator(desc);
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
    Driver driver = new Driver(feedPartition, oper, offsetStorage, cp, offsetCommitInterval);
    recurseOperatorAndDriverNode(desc, driver.getDriverNode());
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
  public static Operator buildOperator(OperatorDesc operatorDesc) {
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
    if (feedDesc.getSpec() == null || "java".equalsIgnoreCase(feedDesc.getSpec())){
      try {
        Constructor<Feed> feedCons = (Constructor<Feed>) Class.forName(feedDesc.getTheClass()).getConstructor(
                paramTypes);
        feed = feedCons.newInstance(feedDesc.getProperties());
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
              | NoSuchMethodException | SecurityException | IllegalArgumentException
              | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    } else if (feedDesc.getSpec().equals("groovy")) {
      try (GroovyClassLoader gc = new GroovyClassLoader()) {
        Class<?> c = gc.parseClass(feedDesc.getScript());
        Constructor<Feed> feedCons = (Constructor<Feed>) c.getConstructor(paramTypes);
        feed = (Feed) feedCons.newInstance(feedDesc.getProperties());

      } catch (InstantiationException | IllegalAccessException | IOException
              | NoSuchMethodException | SecurityException | IllegalArgumentException
              | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    } else if (feedDesc.getSpec().equalsIgnoreCase("url")) {
      List<URL> urls = parseSpecIntoUrlList(feedDesc);
      try (URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[0]))) {
        Class feedClass = loader.loadClass(feedDesc.getTheClass());
        Constructor<Feed> feedCons = (Constructor<Feed>) feedClass.getConstructor(paramTypes);
        feed = feedCons.newInstance(feedDesc.getProperties());
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
              | IOException | NoSuchMethodException | SecurityException | IllegalArgumentException
              | InvocationTargetException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("Do not know what to do with "+feedDesc.getSpec());
    }
    feed.setName(feedDesc.getName());
    feed.setProperties(feedDesc.getProperties());
    return feed;
  }
  
  /**
   * Parse the script information from dynamicInstant... into a List<URL> so a URLClassloader
   * can load it 
   * @param dynamic
   * @return a List of all the non MalformedURLs
   */
  private static List<URL> parseSpecIntoUrlList(DynamicInstantiatable dynamic){
    String [] split = dynamic.getScript().split(",");
    List<URL> urls = new ArrayList<URL>();
    for (String s: split){
      try {
        URL u = new URL(s);
        urls.add(u);
      } catch (MalformedURLException e) { 
        logger.warn("Specified url " + s + "could not be parsed");
      }
    }
    return urls;
  }
}
