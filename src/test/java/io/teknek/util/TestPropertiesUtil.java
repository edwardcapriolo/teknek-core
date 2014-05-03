package io.teknek.util;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class TestPropertiesUtil {
  @Test
  public void onlyCopySome(){  
    Assert.assertEquals(MapBuilder.makeMap("1", "2", "3", "4"), 
            PropertiesUtil.copyOnlyWantedProperties(MapBuilder.makeMap("1", "2", "3", "4", "5", "6"), "1", "3"));
  }
}
