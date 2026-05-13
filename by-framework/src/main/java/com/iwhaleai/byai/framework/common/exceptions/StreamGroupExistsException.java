package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when a stream consumer group already exists.
 */
public class StreamGroupExistsException extends FrameworkException {

    private final String groupName;
    private final String streamName;

    public StreamGroupExistsException(String groupName, String streamName) {
        super("Consumer group already exists: " + groupName + " on stream: " + streamName);
        this.groupName = groupName;
        this.streamName = streamName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getStreamName() {
        return streamName;
    }
}
