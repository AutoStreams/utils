/**
 * Code adapted from:
 * https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/securechat
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
    private final int port;
    private final Logger logger = LoggerFactory.getLogger(DataReceiver.class);
    private final StreamsServer<String> streamsServer;
    private ChannelFuture channelFuture;
    private EventLoopGroup masterGroup = new NioEventLoopGroup(1);
    private EventLoopGroup workerGroup = new NioEventLoopGroup();

    @SuppressWarnings("unused")
    public DataReceiver(StreamsServer<String> streamsServer, int port) {
        this.streamsServer = streamsServer;
        this.port = port;
    }

    /**
     * Execute the data receiver to start listening for messages.
     */
    @SuppressWarnings("unused")
    public void run() {
        ServerBootstrap bootstrap = createServerBootstrap();
        channelFuture = bootstrap.bind(port);

        closeFutureAndShutdown();
    }

    /**
     * Shutdown the data receiver.
     */
    public void shutdown() {
        shutdownChannelFuture();
        shutdownWorkerGroup();
        shutdownMasterGroup();
    }

    private ServerBootstrap createServerBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(this.masterGroup, this.workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new DataReceiverInitializer(streamsServer, this));

        return bootstrap;
    }

    private void closeFutureAndShutdown() {
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

    private void shutdownChannelFuture() {
        if (channelFuture != null) {
            logger.debug("Closing channel future");
            channelFuture.channel().close();
            channelFuture = null;
        }
    }

    private void shutdownWorkerGroup() {
        if (workerGroup != null) {
            logger.debug("Closing worker group");
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }

    private void shutdownMasterGroup() {
        if (masterGroup != null) {
            logger.debug("Closing master group");
            masterGroup.shutdownGracefully();
            masterGroup = null;
        }
    }
}
