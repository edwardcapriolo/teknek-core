package io.teknek.daemon;

public class WorkerStartException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 2709271099304680259L;

  public WorkerStartException() {
    super();
  }

  public WorkerStartException(String message, Throwable cause, boolean enableSuppression,
          boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public WorkerStartException(String message, Throwable cause) {
    super(message, cause);
  }

  public WorkerStartException(String message) {
    super(message);
  }

  public WorkerStartException(Throwable cause) {
    super(cause);
  }

}
