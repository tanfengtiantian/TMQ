package io.kafka.api;
/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:38:32
 * @ClassName 选择主题的分区索引
 */
public interface PartitionChooser {

    int choosePartition(String topic);
}