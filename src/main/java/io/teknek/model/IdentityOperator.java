package io.teknek.model;

/**
 * Clones the fields of a tuple by creating a new tuple, but copies the fields in the old tuple by reference.
 * @author edward
 *
 */
public class IdentityOperator extends Operator {
  public void handleTuple(ITuple t) {
    ITuple tnew = new Tuple();
    for (String field : t.listFields()){
      tnew.setField(field, t.getField(field));
    }
    collector.emit(tnew);
  }
}
