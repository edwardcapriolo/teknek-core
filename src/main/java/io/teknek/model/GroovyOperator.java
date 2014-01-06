package io.teknek.model;

import groovy.lang.Closure;

/**
 * Users can supply operators as groovy closure strings. The GroovyOperator wraps that closure and
 * allow the framework to treat the class as a standard Operator
 * 
 * @author edward
 * 
 */
public class GroovyOperator extends Operator {

  @SuppressWarnings("rawtypes")
  private Closure closure;

  /**
   * Construct an operator that will pass all calls to handleTuple to this closure
   * @param closure
   */
  public GroovyOperator(@SuppressWarnings("rawtypes") Closure closure) {
    this.closure = closure;
  }

  @Override
  public void handleTuple(ITuple t) {
    closure.call(t, getCollector());
  }

}
