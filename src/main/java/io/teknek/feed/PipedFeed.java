package io.teknek.feed;

public class PipedFeed extends FeedPartitionAdapter {

  public PipedFeed(Feed feed, String partitionId) {
    super(feed, partitionId);
  }
  
}
