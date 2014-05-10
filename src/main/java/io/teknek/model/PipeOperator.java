package io.teknek.model;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This operator opens a Pipe to a subprocess connecting the input and output streams.
 * Child classes should interact with the pipe by writing to the outstream, flushing, 
 * and then reading from the input stream. See TestPipeOperator for an example. 
 * @author edward
 *
 */
public abstract class PipeOperator extends Operator {

  public static final String PIPE_OPERATOR_COMMAND = "pipe.operator.command.and.arguments";
  protected CountDownLatch waitForShutdown;
  protected Thread waitForTheEnd;
  protected AtomicInteger exitValue = new AtomicInteger(-9999);
  
  protected InputStream output;
  protected InputStream error; 
  protected OutputStream toProcess;
  protected Process process;
  
  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    String [] commandAndArgs = (String []) properties.get(PIPE_OPERATOR_COMMAND);
    Runtime rt = Runtime.getRuntime();
    final Process process;
    try {
      process = rt.exec(commandAndArgs);
    } catch (IOException e) {
      throw new RuntimeException (e);
    }
    waitForShutdown = new CountDownLatch(1);
    output =  process.getInputStream();
    error = process.getErrorStream();
    toProcess = process.getOutputStream();
    waitForTheEnd = new Thread() {
      public void run() {
        try {
          exitValue.set(process.waitFor());
          waitForShutdown.countDown();
        } catch (InterruptedException e) {
          waitForShutdown.countDown();
        }
      }
    };
    
    waitForTheEnd.start();  
  }

  @Override
  public abstract void handleTuple(ITuple tuple) ;

  @Override
  public void close() {
    if (waitForShutdown == null){
      throw new RuntimeException("Instance is not started. Can not shutdown.");
    }
    process.destroy();
    try {
      waitForShutdown.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
