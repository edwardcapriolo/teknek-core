package io.teknek.driver;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

public class CloseDetectOperator extends Operator {

  public static volatile boolean CLOSED = false;
  
  @Override
  public void handleTuple(ITuple tuple) { }
  
  public void close(){
    CLOSED = true;
  }

}
