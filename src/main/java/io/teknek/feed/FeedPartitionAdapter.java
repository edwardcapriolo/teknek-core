package io.teknek.feed;

import io.teknek.model.ITuple;

/**
 * Extend only the methods you wish with this handy adapter. Generally used for feeds
 * that do not have offset management.
 * @author edward
 *
 */
public class FeedPartitionAdapter extends FeedPartition {

  public FeedPartitionAdapter(Feed feed, String partitionId) {
    super(feed, partitionId);
  }

  @Override
  public void initialize() {
  }

  @Override
  public boolean next(ITuple tupleRef) {
    return false;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean supportsOffsetManagement() {
    return false;
  }

  @Override
  public String getOffset() {
    return null;
  }

  @Override
  public void setOffset(String offset) {
  }

}
