package com.dams.messageparsing.processor;

/**
 * Thrown when a message cannot be processed regardless of retries —
 * e.g. malformed XML or a missing reference-ID path.
 *
 * <p>The batch runner catches this exception, commits the Kafka offset
 * to skip the poison pill, and moves on to the next message.</p>
 */
public class PermanentMessageException extends Exception {

    public PermanentMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
