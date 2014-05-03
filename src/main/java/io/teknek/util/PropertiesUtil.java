package io.teknek.util;

import java.util.HashMap;
import java.util.Map;

public class PropertiesUtil {

  public static Map<String,Object> copyOnlyWantedProperties(Map<String,Object> source, String ... wantedList){
    Map<String,Object> props = new HashMap<String,Object>();
    for (String wanted: wantedList){
      props.put(wanted, source.get(wanted));
    }
    return props;
  }
}
