package io.teknek.driver;

import java.util.Map;

import io.teknek.feed.FeedPartition;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;
import io.teknek.plan.Plan;

public class ImMemoryOffsetStorage extends OffsetStorage {

  private static byte [] value;
  public ImMemoryOffsetStorage(FeedPartition feedPartition, Plan plan, Map<String, String> properties) {
    super(feedPartition, plan, properties);
  }

  public void setValue(String s){
    value = s.getBytes();
  }
  
  @Override
  public void persistOffset(Offset o) {
    value = o.serialize();
  }

  @Override
  public Offset getCurrentOffset() {
    final String offset = feedPartiton.getOffset();
    return new Offset(value){

      @Override
      public byte[] serialize() {
        return offset.getBytes();
      } 
    };
  }

  @Override
  public Offset findLatestPersistedOffset() {
    return new Offset(value) {
      @Override
      public byte[] serialize() {
        return value;
      }
    };
  }

}
