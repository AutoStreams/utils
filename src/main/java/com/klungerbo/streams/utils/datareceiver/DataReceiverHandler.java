/*
 * Code adapted from:
 * https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/securechat
 */

package com.klungerbo.streams.utils.datareceiver;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for Data receiver which manages client connection/disconnection
 * and incoming messages.
 *
 * @version 1.0
 * @since 1.0
 */
public class DataReceiverHandler extends SimpleChannelInboundHandler<String> {
    private static final String DISCONNECT_COMMAND = "streams_command_disconnect";
    private static final String SHUTDOWN_COMMAND = "streams_command_shutdown";

    private static final ChannelGroup channels =
        new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final DataReceiver dataReceiver;
    private final StreamsServer<String> streamsServer;
    private final Logger logger = LoggerFactory.getLogger(DataReceiverHandler.class);

    /**
     * Create a DataReceiverHandler instance with injected KafkaPrototypeProducer.
     */
    public DataReceiverHandler(
        @NotNull StreamsServer<String> streamsServer,
        @NotNull DataReceiver dataReceiver) {
        this.streamsServer = streamsServer;
        this.dataReceiver = dataReceiver;
    }

    /**
     * Handle a new incoming client connection.
     *
     * @param context the interaction context to the pipeline.
     * @throws UnknownHostException if the host could not be determined by its IP.
     */
    @Override
    public void handlerAdded(@NotNull ChannelHandlerContext context) throws UnknownHostException {
        logger.debug("Adding channel: {}", context.channel().id());
        context.writeAndFlush("Connected to: " + InetAddress.getLocalHost().getHostName() + "\n");
        channels.add(context.channel());

        logger.debug("Channels:");
        for (Channel channel : channels) {
            logger.debug("{}", channel.id());
        }
    }

    /**
     * Handle a removed client connection.
     *
     * @param context the interaction context to the pipeline.
     */
    @Override
    public void handlerRemoved(@NotNull ChannelHandlerContext context) {
        logger.debug("Removing channel: {}", context.channel().id());
        channels.remove(context.channel());

        logger.debug("Remaining channels:");
        for (Channel channel : channels) {
            logger.debug("{}", channel.id());
        }
    }

    /**
     * Handle incoming message from a client.
     *
     * @param context the interaction context to the pipeline.
     * @param message the message received from the client.
     */
    @Override
    protected void channelRead0(@NotNull ChannelHandlerContext context, @NotNull String message) {
        logger.info("Received message: {}", message);

        if (DISCONNECT_COMMAND.equalsIgnoreCase(message)) {
            closeChannel(context.channel());
        } else if (SHUTDOWN_COMMAND.equalsIgnoreCase(message)) {
            this.shutdown();
        } else {
            streamsServer.onMessage(message);
        }
    }

    /**
     * Close a channel connected to the server.
     *
     * @param channel the channel to close.
     */
    private void closeChannel(@NotNull Channel channel) {
        logger.info("Closing channel: {}", channel.id());

        channel.parent().writeAndFlush("Disconnected\n");
        channel.parent().close();
    }

    /**
     * Close all channels connected to the server.
     */
    private void closeChannels() {
        for (Channel channel : channels) {
            try {
                channel.writeAndFlush(SHUTDOWN_COMMAND + "\n");
                channel.close().sync();
            } catch (InterruptedException e) {
                logger.error("Thread interrupted");
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Shutdown the server.
     */
    private void shutdown() {
        logger.info("Shutting down");

        this.closeChannels();
        this.dataReceiver.shutdown();
        streamsServer.onShutdown();
    }

    /**
     * Handle exception.
     *
     * @param context the interaction context to the pipeline.
     * @param cause   the cause of the exception.
     */
    @Override
    public void exceptionCaught(@NotNull ChannelHandlerContext context, @NotNull Throwable cause) {
        cause.printStackTrace();
        context.close();
    }
}