package com.pms.rttm.client.exception;

import lombok.NoArgsConstructor;

/**
 * Exception thrown when RTTM client operations fail.
 * Wraps underlying exceptions and provides context about the failed operation.
 */
@NoArgsConstructor
public class RttmClientException extends Exception {

    public RttmClientException(String message) {
        super(message);
    }

    public RttmClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public RttmClientException(Throwable cause) {
        super(cause);
    }
}
