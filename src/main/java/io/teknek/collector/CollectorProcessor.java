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
package io.teknek.collector;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Threaded component responsible for pulling tupple from collector and forwarding it to eachc child
 * operator
 * 
 * @author edward
 * 
 */
public class CollectorProcessor implements Runnable {
  final static Logger logger = Logger.getLogger(CollectorProcessor.class.getName());

  private Collector collector;

  //TODO benchmark this as an array
  private List<Operator> children;

  private volatile boolean goOn = true;

  private int tupleRetry;

  public CollectorProcessor() {
    children = new ArrayList<Operator>();
    collector = new Collector();
  }

  /**
   * Take tuple from the collector (blocking) call handleTuple
   */
  public void run() {
    while (goOn) {
      takeOne();
    }
  }

  @VisibleForTesting
  public void takeOne(){
    try {
      ITuple tuple = collector.take();
      handleTupple(tuple);
    } catch (InterruptedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("While fetching tuple", e);
      }
      throw new RuntimeException(e);
    }
  }
  @VisibleForTesting
  /**
   * For each child operator, attempt up to tupleRetry times to call operator.handleTuple(). If the framework tries 
   * tuple retry times unsuccessfully discard the tuple.
   * @param tuple
   */
  public void handleTupple(ITuple tuple) {
    if (children.size() == 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("No children operators for this operator. Tuple not being passed on " + tuple);
      }
      return;
    }
    for (Operator o : children) {
      int attemptCount = 0;
      while (attemptCount++ < tupleRetry + 1) {
        try {
          o.handleTuple(tuple);
          break;
        } catch (RuntimeException ex) {
          logger.debug("Exception handling tupple " + tuple, ex);
        }
      }
    }
  }

  public Collector getCollector() {
    return collector;
  }

  public List<Operator> getChildren() {
    return children;
  }

  public boolean isGoOn() {
    return goOn;
  }

  public void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }

  public int getTupleRetry() {
    return tupleRetry;
  }

  public void setTupleRetry(int tupleRetry) {
    this.tupleRetry = tupleRetry;
  }

}
