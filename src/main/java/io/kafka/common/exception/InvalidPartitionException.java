package io.kafka.common.exception;

import io.kafka.common.ErrorMapping;

public class InvalidPartitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidPartitionException() {
        super();
    }

    public InvalidPartitionException(String message) {
        super(message);
    }

}
