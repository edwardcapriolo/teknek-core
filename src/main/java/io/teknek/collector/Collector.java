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

import io.teknek.model.ICollector;
import io.teknek.model.ITuple;

import java.util.concurrent.ArrayBlockingQueue;

 
/**
 * Positioned between two operators. The methods emit() take() and peek() work
 * on an underlying blocking queue which provides flow control.
 *
 */
public class Collector extends ICollector {

  //TODO this needs to be made configurable via parameters
  public static final int DEFAULT_QUEUE_SIZE = 4000;
  private ArrayBlockingQueue<ITuple> collected;

  public Collector(){
    collected = new ArrayBlockingQueue<ITuple>(DEFAULT_QUEUE_SIZE);
  }
  
  @Override
  public void emit(ITuple out) {
    try {
      collected.put(out);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Take the next tuple off the queue blocking until the next arrives
   * @return
   * @throws InterruptedException
   */
  public ITuple take() throws InterruptedException{
    return collected.take();
  }
  
  /**
   * Peek at the next tuple without pulling it off the queue
   * @return
   * @throws InterruptedException
   */
  public ITuple peek() throws InterruptedException{
    return collected.peek();
  }
  
  /** returns size of the queue managed by this object */
  public int size() {
    return collected.size();
  }
}
