package com.stormpath.samza.lang;

/**
 * @since 0.1
 */
public class InstantiationException extends RuntimeException {

    public InstantiationException(String s, Throwable t) {
        super(s, t);
    }
}
