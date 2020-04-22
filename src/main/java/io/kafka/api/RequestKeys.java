package io.kafka.api;
/**
 * @author tf
 * @version 创建时间：2019年1月17日 下午12:05:53
 * @ClassName Request接入类型
 */
public enum RequestKeys {
    PRODUCE, //0
    FETCH, //1
    MULTIFETCH, //2
    MULTIPRODUCE, //3
    OFFSETS,//4
    /**
     * 事务消息
     */
    TRANSACTION,//5
    /**
     * 延迟消息
     */
    TTLPRODUCE,//6
    /** 创建更多分区
     *
     */
    CREATE,//7
    /**
     * 删除未使用的主题
     *
     */
    DELETE,//8
    /**
     * rpc协议
     *
     */
    RPC,//9
    ;

    //
    public int value = ordinal();

    //
    final static int size = values().length;

    public static RequestKeys valueOf(int ordinal) {
        if (ordinal < 0 || ordinal >= size) return null;
        return values()[ordinal];
    }
}