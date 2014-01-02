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
package io.teknek.model;

import java.util.Map;

/**
 * Represents processing logic. An operator takes a tuple as input and emits 0 or more tuples to the
 * output collector.
 * 
 */
public abstract class Operator {

  /**
   * Configuration properties that are passed at initialization to the operator
   */
  protected Map<String, Object> properties;

  /**
   * Container that holds tuples emitted from the operator
   */
  protected ICollector collector;

  public Operator() {
    super();
  }

  /**
   * Sets the properties of the operator. Called once after object construction and before the plan
   * is started
   * 
   * @param properties
   */
  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  /**
   * Do some type of processing on this tuple. Note in many cases if this operator emit's a tuple a
   * new object should be created.
   * 
   * @param tuple
   *          an input tuple
   */
  public abstract void handleTuple(ITuple tuple);

  public void setCollector(ICollector i) {
    this.collector = i;
  }

  public ICollector getCollector() {
    return this.collector;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

}
