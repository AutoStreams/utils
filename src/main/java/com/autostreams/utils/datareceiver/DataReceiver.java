/**
 * Code adapted from:
 * <a href="https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/securechat">https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/securechat</a>.
 */

package com.autostreams.utils.datareceiver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data receiver (server) that listens for messages from data producers (clients).
 * Messages are delegated to a Kafka prototype producer.
 *
 * @version 1.0
 * @since 1.0
 */
public class DataReceiver {
    private static final int PORT = Integer.parseInt(System.getProperty("port", "8992"));
    private final Logger logger = LoggerFactory.getLogger(DataReceiver.class);
    private final StreamsServer<String> streamsServer;
    private ChannelFuture channelFuture;
    private EventLoopGroup masterGroup;
    private EventLoopGroup workerGroup;

    public DataReceiver(StreamsServer<String> streamsServer) {
        this.streamsServer = streamsServer;
    }

    /**
     * Execute the data receiver to start listening for messages.
     */
    @SuppressWarnings("unused")
    public void run() {
        masterGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = createServerBootstrap();
        channelFuture = bootstrap.bind(PORT);

        receiveMessages();
    }

    /**
     * Start receiving messages.
     */
    private void receiveMessages() {
        try {
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            logger.error("Thread interrupted");
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            this.shutdown();
        }
    }

    /**
     * Create a new server bootstrap from predefined configurations.
     *
     * @return the newly created bootstrapped server
     */
    private ServerBootstrap createServerBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(masterGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new DataReceiverInitializer(streamsServer, this));

        return bootstrap;
    }

    /**
     * Shutdown the data receiver.
     */
    public void shutdown() {
        shutdownChannelFuture();
        shutdownWorkerGroup();
        shutdownMasterGroup();
    }

    /**
     * Shutdown the channel future used for message listening.
     */
    private void shutdownChannelFuture() {
        if (channelFuture != null) {
            logger.debug("Closing channel future");
            channelFuture.channel().close();
            channelFuture = null;
        }
    }

    /**
     * Shutdown the worker group
     */
    private void shutdownWorkerGroup() {
        if (workerGroup != null) {
            logger.debug("Closing worker group");
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }

    /**
     * Shutdown the master group
     */
    private void shutdownMasterGroup() {
        if (masterGroup != null) {
            logger.debug("Closing master group");
            masterGroup.shutdownGracefully();
            masterGroup = null;
        }
    }
}
