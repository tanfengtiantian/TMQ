package io.kafka.log;

public interface LogSegmentFilter {

    boolean filter(ILogSegment segment);
}
