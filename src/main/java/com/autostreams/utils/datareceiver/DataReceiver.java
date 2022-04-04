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
    public void run() {
        masterGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(masterGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new DataReceiverInitializer(streamsServer, this));

        channelFuture = bootstrap.bind(PORT);

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
     * Shutdown the data receiver.
     */
    public void shutdown() {
        if (channelFuture != null) {
            logger.debug("Closing channel future");
            channelFuture.channel().close();
            channelFuture = null;
        }

        if (workerGroup != null) {
            logger.debug("Closing worker group");
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }

        if (masterGroup != null) {
            logger.debug("Closing master group");
            masterGroup.shutdownGracefully();
            masterGroup = null;
        }
    }
}
