apm消息缓冲服务器

# 消息的存储结构

消息在服务器的是按照顺序连续append在一起的，具体的单个消息的存储结构如下：

* message length(4 bytes),包括消息属性和payload data
* checksum(4 bytes)
* message id(8 bytes)
* message flag(4 bytes)
* attribute length(4 bytes) + attribute，可选
* payload

其中checksum采用CRC32算法计算，计算的内容包括消息属性长度+消息属性+data，消息属性如果不存在则不包括在内。消费者在接收到消息后会检查checksum是否正确。

同一个topic下有不同分区，每个分区下面会划分为多个文件，只有一个当前文件在写，其他文件只读。当写满一个文件（写满的意思是达到设定值）则切换文件，新建一个当前文件用来写，老的当前文件切换为只读。文件的命名以起始偏移量来命名。看一个例子，假设test这个topic下的0-0分区可能有以下这些文件：

* 00000000000000000000000000000000.tmq
* 00000000000000000000000000001024.tmq
* 00000000000000000000000000002048.tmq
* ……

其中00000000000000000000000000000000.tmq表示最开始的文件，起始偏移量为0。第二个文件00000000000000000000000000001024.tmq的起始偏移量为1024，同时表示它的前一个文件的大小为1024-0=1024。同样，第三个文件00000000000000000000000000002048.tmq的起始偏移量为2048，表明00000000000000000000000000001024.tmq的大小为2048-1024=1024。

以起始偏移量命名并排序这些文件，那么当消费者要抓取某个起始偏移量开始位置的数据变的相当简单，只要根据传上来的offset**二分查找**文件列表，定位到具体文件，然后将绝对offset减去文件的起始节点转化为相对offset，即可开始传输数据。例如，同样以上面的例子为例，假设消费者想抓取从1536开始的数据1M，则根据1536二分查找，定位到00000000000000000000000000001024.tmq这个文件（1536在1024和2048之间），1536-1024=512，也就是实际传输的起始偏移量是在00000000000000000000000000001024.tmq文件的512位置。因为1024.tmq的大小才1K，比1M小多了，实际传输的数据只有2048-1536=512字节。

这些文件在Queue里命名为Segment，每个Segment对应一个FileMessageSet。文件组织成SegmentList，整体成为一个MessageStore，一个topic下的一个分区对应一个MessageStore。

                   topic
                 /     \
          partition1   partition2
              |          ......
             Log
              |
        LogSegmentList
       /      |       \
    Segment Segment Segment ......


# 可靠性、顺序和重复

## 可靠性
tmq的可靠性保证贯穿客户端和服务器。

### 生产者的可靠性保证
消息生产者发送消息后返回SendResult，如果isSuccess返回为true,则表示消息已经确认发送到服务器并被服务器接收存储。整个发送过程是一个同步的过程。保证消息送达服务器并返回结果。

### 服务器的可靠性保证
消息生产者发送的消息，tmq服务器收到后在做必要的校验和检查之后的第一件事就是写入磁盘，写入成功之后返回应答给生产者。因此，可以确认每条发送结果为成功的消息服务器都是写入磁盘的。

写入磁盘，不意味着数据落到磁盘设备上，毕竟我们还隔着一层os，os对写有缓冲。tmq有两个特性来保证数据落到磁盘上

* 每1000条（可配置），即强制调用一次force来写入磁盘设备。
* 每隔10秒（可配置），强制调用一次force来写入磁盘设备。

因此,tmq通过配置可保证在异常情况下（如磁盘掉电）10秒内最多丢失1000条消息。当然通过参数调整你甚至可以在掉电情况下不丢失任何消息。

服务器通常组织为一个集群，一条从生产者过来的消息可能按照路由规则存储到集群中的某台机器。

### 消费者的可靠性保证
消息的消费者是一条接着一条地消费消息，只有在成功消费一条消息后才会接着消费下一条。如果在消费某条消息失败（如异常），则会尝试重试消费这条消息（默认最大5次），超过最大次数后仍然无法消费，则将消息存储在消费者的本地磁盘，由后台线程继续做重试。而主线程继续往后走，消费后续的消息。因此，只有在MessageListener确认成功消费一条消息后，tmq的消费者才会继续消费另一条消息。由此来保证消息的可靠消费。

消费者的另一个可靠性的关键点是offset的存储，也就是拉取数据的偏移量。我们目前提供了以下几种存储方案

* zookeeper，默认存储在zoopkeeper上，zookeeper通过集群来保证数据的安全性。
* file，文件存储，将offset信息存储在消费者的本地文件中。

Offset会定期保存，并且在每次重新负载均衡前都会强制保存一次。



## 顺序

很多人关心的消息顺序，希望消费者消费消息的顺序跟消息的发送顺序是一致的。比如，我发送消息的顺序是A、B、C，那么消费者消费的顺序也应该是A、B、C。乱序对某些应用可能是无法接受的。

tmq对消息顺序性的保证是有限制的，默认情况下，消息的顺序以谁先达到服务器并写入磁盘，则谁就在先的原则处理。并且，发往同一个分区的消息保证按照写入磁盘的顺序让消费者消费，这是因为消费者针对每个分区都是按照从前到后递增offset的顺序拉取消息。

tmq可以保证，在单线程内使用该producer发送的消息按照发送的顺序达到服务器并存储，并按照相同顺序被消费者消费，前提是这些消息发往同一台服务器的同一个分区。为了实现这一点，你还需要实现自己的PartitionSelector用于固定选择分区

    public interface PartitionSelector {
        public Partition getPartition(String topic, List<Partition> partitions, Message message) throws ClientException;
    }

选择分区可以按照一定的业务逻辑来选择，如根据业务id来取模。或者如果是传输文件，可以固定选择第n个分区使用。当然，如果传输文件，通常我们会建议你只配置一个分区，那也就无需选择了。


## 消息重复

消息的重复包含两个方面，生产者重复发送消息以及消费者重复消费消息。

针对生产者来说，有可能发生这种情况，生产者发送消息，等待服务器应答，这个时候发生网络故障，服务器实际已经将消息写入成功，但是由于网络故障没有返回应答。那么生产者会认为发送失败，则再次发送同一条消息，如果发送成功，则服务器实际存储两条相同的消息。这种由故障引起的重复，是无法避免的，因为不判断消息的data是否一致，因为它并不理解data的语义，而仅仅是作为载荷来传输。

针对消费者来说也有这个问题，消费者成功消费一条消息，但是此时断电，没有及时将前进后的offset存储起来，则下次启动的时候或者其他同个分组的消费者owner到这个分区的时候，会重复消费该条消息。这种情况也无法完全避免。

消息重复的保证只能说在正常情况下保证不重复，异常情况无法保证，这些限制是由远程调用的语义引起的，要做到完全不重复的代价很高。

## 事务消息

事务的实现主要是如下几个类：

* TransactionHandler：事务命令处理器。它主要作用是接收client端发过来的各种请求，如beginTransaction、prepare、commit、rollback、新增消息等。
* JournalTransactionStore：基于文件方式的事务存储引擎。
* JournalStore：具体存储事务的文件存储类。

它们3者之间的关系是：TransactionHandler接收client端的各种事务请求，然后调用JournalTransactionStore进行事务存储，JournalTransactionStore根据不同的client请求调用JournalStore具体保存事务信息到磁盘文件。

下面进一步进行说明：

client通过调用beginTransaction来新开始一个事务(在tmq里就是一个Tx实例),并把它放在JournalTransactionStore类的inflightTransactions队列里，然后client就可以在这个Tx中新增消息，但这些新增的消息是放在JournalStore文件里，并且完整的保存在内存中(由于目前没有专门的内存管理机制，当事务数量特别大的时候，这个地方有可能会出现内存溢出)。当client进行2PC中的prepare时，事务从inflightTransactions队列移到preparedTransactions队列，并保存相关信息到JournalStore。当执行commit时，该Tx的所有消息才真正放到MessageStore里供消息消费者读取。当client端发起rollback请求后，Tx被从preparedTransactions队列中删除，并保存相关信息到JournalStore。

下面我们对事务实现的几个重要方面做一个详细介绍：

#### beginTransaction

当client端的TransactionContext调用start方法，broker接收到请求后，启动一个新事务。
#### 新增消息

当事务begin后，client端向broker发送多条消息存储的请求，broker收到请求后会调用JournalTransactionStore的addMessage方法。该方法把请求存储在事务日志文件中(JournalStore)，同时新建或找到对应的Tx实例，把这些消息存储请求保存在内存中。这里注意一点，在事务没有提交之前，这些消息存储是不会被放到对应topic消息存储文件中去的。

####prepare的处理过程

prepare的处理过程相对简单些，它只是把Tx实例从JournalTransactionStore类的inflightTransactions中移除到preparedTransactions中，同时在事务日志文件存储相关信息。

#### commit的处理过程

commit过程相当复杂点。broker收到client端的commit请求，调用JournalTransactionStore的commit方法，从preparedTransactions里找到对应的Tx,把该Tx里的所有请求命令(PutCommand)，按照topic和分区分别保存到真正的topic消息存储文件中去，当全部保存完时，就会通过回调类AppendCallback的appendComplete方法记录commit日志到事务日志文件。

#### recover的处理过程

recover操作发生在系统重启的时候，主要是为了还原系统上一次停止时候的事务场景，如还原处在prepare阶段的事务，rollback所有本地事务。recover的处理细节包括两部分：

* 在JournalTransactionStore的构造函数中进行JournalStore的recover操作
* JournalStore的recover主要是完成从事务日志文件中按照最近的日志中读取所有的日志记录，并按照记录的类型APPEND_MSG和TX_OP分别进行还原操作：
##### APPEND_MSG类型
这种类型的日志记录就调用JournalTransactionStore的addMessage方法，但是不会往日志文件中重复记录该消息了。
##### TX_OP类型
这种类型的处理相当复杂点。它根据日志记录的类型又细分为下面几种

* LOCAL_COMMIT:根据TransactionId从JournalTransactionStore类的inflightTransactions或preparedTransactions中找到对应的Tx实例。把该Tx内的所有消息请求对比相应topic消息存储文件中消息，如果topic消息存储文件中不存在这些消息则新增，如果存在则通过crc32校验码进行比对。
* LOCAL_ROLLBACK:根据TransactionId把对应的Tx实例从JournalTransactionStore类的inflightTransactions或preparedTransactions中删除。

经过上面的recover操作后，它已经把重启前的事务现场在JournalTransactionStore和JournalStore中进行了还原。接下来就是TransactionalCommandProcessor类的事务现场还原，这个过程是把JournalTransactionStore类的preparedTransactions中的所有Tx在TransactionalCommandProcessor中进行还原，该过程相对简单，可参考源码实现。

## 延迟队列

延迟队列的实现主要是如下几个类：
* HashedWheelTimer：底层数据结构依然是使用DelayedQueue。加上一种叫做时间轮的算法来实现。
关于时间轮算法，有点类似于HashMap。在new 一个HashedWheelTimer实例的时候，可以传入几个参数。

第一，一个时间长度，这个时间长度跟具体任务何时执行没有关系，但是跟执行精度有关。这个时间可以看作手表的指针循环一圈的长度。

然后第二，刻度数。这个可以看作手表的刻度。比如第一个参数为24小时，刻度数为12，那么每一个刻度表示2小时。时间精度只能到两小时。时间长度/刻度数值越大，精度越大。



然后添加一个任务的时候，根据hash算法得到hash值并对刻度数求模得到一个下标，这个下标就是刻度的位置。

然而有一些任务的执行周期超过了第一个参数，比如超过了24小时，就会得到一个圈数round。

简点说，添加一个任务时会根据任务得到一个hash值，并根据时间轮长度和刻度得到一个商值round和模index，比如时间长度24小时，刻度为12，延迟时间为32小时，那么round=1,index=8。时间轮从开启之时起每24/12个时间走一个指针，即index+1,第一圈round=0。当走到第7个指针时，此时index=7，此时刚才的任务并不能执行，因为刚才的任务round=1,必须要等到下一轮index=7的时候才能执行。

delay值的设置是按照下面规则进行的：

![tmq](https://images2018.cnblogs.com/blog/600147/201712/600147-20171202215241038-1756544686.png)


对于Delayed两个重要实现方法，第一排序，其实是通过hash求商和模决定放入哪个位置。这些位置本身就已经按照时间顺序排序了。
  第一排序，其实是通过hash求商和模决定放入哪个位置。这些位置本身就已经按照时间顺序排序了。
  第二，延迟时间，已经被封装好了，传入一个延迟的时间就好了。

* PooledByteBufAllocator：为了避免频繁的内存分配给系统带来负担以及GC对系统性能带来波动，tmq参考Netty4使用了内存池来管理内存的分配和回收。

#### 内存数据结构
   内存分级从上到下主要分为: Arena、ChunkList、Chunk、Page、SubPage，他们关系如下图

  ![tmq](https://raw.githubusercontent.com/RobertoHuang/RGP-IMAGE/master/%E6%AD%BB%E7%A3%95NETTY%E6%BA%90%E7%A0%81/%E6%AD%BB%E7%A3%95Netty%E6%BA%90%E7%A0%81%E4%B9%8B%E5%86%85%E5%AD%98%E5%88%86%E9%85%8D%E8%AF%A6%E8%A7%A3/Netty%E5%86%85%E5%AD%98%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%8401.png)
  ![tmq](https://raw.githubusercontent.com/RobertoHuang/RGP-IMAGE/master/%E6%AD%BB%E7%A3%95NETTY%E6%BA%90%E7%A0%81/%E6%AD%BB%E7%A3%95Netty%E6%BA%90%E7%A0%81%E4%B9%8B%E5%86%85%E5%AD%98%E5%88%86%E9%85%8D%E8%AF%A6%E8%A7%A3/Netty%E5%86%85%E5%AD%98%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%8402.png)

   PooledArena是一块连续的内存块，为了优化并发性能在Netty内存池中存在一个由多个Arena组成的数组，在多个线程进行内存分配时会按照轮询策略选择一个Arena进行内存分配。一个PoolArena内存块是由两个SubPagePools(用来存储零碎内存)和多个ChunkList组成，两个SubpagePools数组分别为tinySubpagePools和smallSubpagePools。每个ChunkList里包含多个Chunk按照双向链表排列，每个Chunk里包含多个Page(默认2048个)，每个Page(默认大小为8k字节)由多个Subpage组成。Subpage由M个”块”构成，块的大小由第一次申请内存大小决定。当分配一次内存之后此page会被加入到PoolArena的tinySubpagePools或smallSubpagePools中，下次分配时就如果”块”大小相同则由其直接分配

   当利用Arena来进行分配内存时，根据申请内存的大小有不同的策略。例如:如果申请内存的大小小于512时，则首先在cache尝试分配，如果分配不成功则会在tinySubpagePools尝试分配，如果分配不成功，则会在PoolChunk重新找一个PoolSubpage来进行内存分配，分配之后将此PoolSubpage保存到tinySubpagePools中