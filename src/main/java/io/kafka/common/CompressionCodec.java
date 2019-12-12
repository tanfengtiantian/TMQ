package io.kafka.common;

import io.kafka.common.exception.UnKnownCodecException;


/**
 * @author tf
 * @version 创建时间：2019年1月10日 上午9:35:35
 * @ClassName 消息压缩类型
 * @Description 类描述
 */
public enum CompressionCodec {
    /**
     * Not compression
     */
    NoCompressionCodec(0), //
    /**
     * GZIP compression
     */
    GZIPCompressionCodec(1), //
    /**
     * Snappy compression (NOT USED)
     */
    SnappyCompressionCodec(2), //
    /**
     * DEFAULT compression(equals GZIPCompressionCode)
     */
    DefaultCompressionCodec(1);

    /**
     * the codec value
     */
    public final int codec;

    CompressionCodec(int codec) {
        this.codec = codec;
    }

    //
    public static CompressionCodec valueOf(int codec) {
        switch (codec) {
            case 0:
                return NoCompressionCodec;
            case 1:
                return GZIPCompressionCodec;
            case 2:
                return SnappyCompressionCodec;
        }
        throw new UnKnownCodecException("unkonw codec: " + codec);
    }
}
