/*
 * Code adapted from:
 * https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/securechat
 */

package com.autostreams.utils.dataprovider;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * Pipeline initializer for data producer.
 *
 * @version 1.0
 * @since 1.0
 */
public class DataProducerInitializer extends ChannelInitializer<SocketChannel> {
    DataProvider dataProvider;

    DataProducerInitializer(DataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    /**
     * Initialize a channel's pipeline.
     *
     * @param channel the socket channel to initialize the pipeline on.
     */
    @Override
    public void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
        pipeline.addLast(new StringDecoder());
        pipeline.addLast(new StringEncoder());
        pipeline.addLast(new DataProducerHandler(this.dataProvider));
    }
}