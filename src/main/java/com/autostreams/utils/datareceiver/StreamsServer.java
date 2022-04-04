package com.autostreams.utils.datareceiver;

/**
 * Interface for Streams server.
 *
 * @param <T> the message type.
 */
public interface StreamsServer<T> {
    void onMessage(T message);

    void onShutdown();
}
