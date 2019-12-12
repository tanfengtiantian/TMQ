package io.kafka.common;

import io.kafka.common.exception.InvalidMessageException;
import io.kafka.common.exception.InvalidMessageSizeException;
import io.kafka.common.exception.InvalidPartitionException;
import io.kafka.common.exception.OffsetOutOfRangeException;

import java.nio.ByteBuffer;

/**
 * @author tf
 * @version 创建时间：2019年1月24日 上午10:12:17
 * @ClassName 错误代码和异常的双向映射
 */
public enum ErrorMapping {

    UnkonwCode(-1), //未知代码
    NoError(0), //
    OffsetOutOfRangeCode(1), //偏移超出范围代码
    InvalidMessageCode(2), //无效的消息代码
    WrongPartitionCode(3), //错误的分区代码
    InvalidFetchSizeCode(4);//无效的fetchSize代码

    public final short code;

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    ErrorMapping(int code) {
        this.code = (short) code;
    }

    public static ErrorMapping valueOf(Exception e) {
        Class<?> clazz = e.getClass();
        if (clazz == OffsetOutOfRangeException.class) {
            return OffsetOutOfRangeCode;
        }
        if (clazz == InvalidMessageException.class) {
            return InvalidMessageCode;
        }
        if (clazz == InvalidPartitionException.class) {
            return WrongPartitionCode;
        }
        if (clazz == InvalidMessageSizeException.class) {
            return InvalidFetchSizeCode;
        }
        return UnkonwCode;
    }

    public static ErrorMapping valueOf(short code) {
        switch (code) {
            case 0:
                return NoError;
            case 1:
                return OffsetOutOfRangeCode;
            case 2:
                return InvalidMessageCode;
            case 3:
                return WrongPartitionCode;
            case 4:
                return InvalidFetchSizeCode;
            default:
                return UnkonwCode;
        }
    }
}
