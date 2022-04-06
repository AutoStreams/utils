/**
 * Code adapted from:
 * https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/securechat
 */

package com.autostreams.utils.dataprovider;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data producer (client) that connects to a producer (server) in order to send messages.
 *
 * @version 1.0
 * @since 1.0
 */
public final class DataProvider {
    private final Logger logger = LoggerFactory.getLogger(DataProvider.class);
    private final Bootstrap bootstrap = new Bootstrap();
    private final Lorem lorem = LoremIpsum.getInstance();
    private final String host;
    private final int port;
    private EventLoopGroup group = new NioEventLoopGroup();
    private boolean running = true;
    private ChannelFuture channelFuture = null;
    private int messagesPerSecond = 1;

    /**
     * Data provider constructor.
     *
     * @param host the host to connect to (URL)
     * @param port the port to connect to
     */
    public DataProvider(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Creates a new data provider.
     *
     * @param host the host to connect to (URL)
     * @param port the port to connect to
     * @return the constructed data provider
     */
    public static DataProvider fromHostAndPort(String host, int port) {
        return new DataProvider(host, port);
    }

    /**
     * Initialize the DataProducer.
     *
     * @return true on success, false if else
     */
    public boolean initialize() {
        bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .handler(new DataProducerInitializer(this));

        int tries = 100;
        int currentTry = 1;
        int secondsToSleep = 5;
        boolean connected = false;

        logger.info("Connecting to: {}:{}", host, port);
        while (!connected && currentTry <= tries) {
            try {
                this.channelFuture = bootstrap.connect(host, port).sync();
                connected = true;
            } catch (InterruptedException e) {
                logger.warn("Connection was interrupted");
                e.printStackTrace();
                Thread.currentThread().interrupt();

                return false;
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn(
                    "[{}/{}] Failed to initialize DataProducer, retrying in {} seconds",
                    currentTry,
                    tries,
                    secondsToSleep
                );
            }

            try {
                TimeUnit.SECONDS.sleep(secondsToSleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();

                return false;
            }

            currentTry++;
        }

        return true;
    }

    /**
     * Generate a random lorem ipsum string.
     *
     * @return a random lorem ipsum string of min <= n <= max amount of n words.
     */
    private String getRandomString() {
        return lorem.getWords(7, 12);
    }

    /**
     * sets the number of messages per second.
     * NOTE: Needs to be greater than 0.
     *
     * @param messagesPerSecond number of messages per second
     */
    public void setMessagesPerSecond(int messagesPerSecond) {
        if (messagesPerSecond <= 0) {
            throw new IllegalArgumentException("Number of messages per second needs to be above 0");
        }

        this.messagesPerSecond = messagesPerSecond;
    }

    /**
     * Execute the DataProducer.
     */
    public void run() {
        while (this.running) {
            String line = getRandomString();
            logger.info("String created: {}", line);

            if (this.channelFuture != null) {
                this.channelFuture = this.channelFuture.channel().writeAndFlush(line + "\r\n");
            }

            try {
                TimeUnit.MILLISECONDS.sleep(1000 / messagesPerSecond);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Shutdown the DataProducer.
     */
    public void shutdown() {
        this.running = false;
        logger.info("Shutting down");

        if (channelFuture != null) {
            logger.debug("Closing channel future");
            channelFuture.channel().close();
            channelFuture = null;
        }

        if (group != null) {
            logger.debug("Closing group");
            group.shutdownGracefully();
            group = null;
        }
    }
}