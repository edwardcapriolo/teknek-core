package io.teknek.operator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;

import io.teknek.model.ITuple;
import io.teknek.model.Tuple;

public class SimplePipeOperator extends PipeOperator{

  public static final String INPUT_FIELD = "simple.pipe.operator.input.field";
  public static final String OUTPUT_FIELD = "simple.pipe.operator.output.field";
  private String inputField;
  private String outputField;
  private BufferedReader br;
  private BufferedWriter bw;
  
  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    inputField = (String) properties.get(INPUT_FIELD);
    outputField = (String) properties.get(OUTPUT_FIELD);
    if (inputField == null){
      inputField = "input";
    }
    if (outputField == null){
      outputField = "output";
    }
    br = new BufferedReader(new InputStreamReader(output));
    bw = new BufferedWriter(new OutputStreamWriter(toProcess));
  }

  @Override
  public void handleTuple(ITuple tuple) {
    try {
      bw.write(tuple.getField(inputField) instanceof String ? (String)tuple.getField(inputField) : tuple.getField(inputField).toString());
      bw.write("\n");
      bw.flush();
      collector.emit(new Tuple().withField(outputField, br.readLine()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}