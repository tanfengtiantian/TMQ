/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.kafka.utils.nettyloc;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class PooledByteBufAllocator extends AbstractByteBufAllocator {

    private static final int DEFAULT_NUM_DIRECT_ARENA;

    private static final int DEFAULT_PAGE_SIZE;
    private static final int DEFAULT_MAX_ORDER; // 8192 << 11 = 16 MiB per chunk

    private static final int MIN_PAGE_SIZE = 4096;
    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    static {
        // 1.对DEFAULT_PAGE_SIZE进行初始化默认是8K
        // 用户可以通过设置io.netty.allocator.pageSize来设置
        int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
        try {
            // 2.检查pageSize是否大于MIN_PAGE_SIZE(4K)且是2的幂次方
            validateAndCalculatePageShifts(defaultPageSize);
        } catch (Throwable t) {
            defaultPageSize = 8192;
        }
        DEFAULT_PAGE_SIZE = defaultPageSize;

        // 3.对树的深度DEFAULT_MAX_ORDER进行初始化 默认是11
        // 用户可以通过io.netty.allocator.maxOrder来进行设置
        int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 11);
        try {
            // 4.校验maxOrder 期望值(0-14之间)
            validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
        } catch (Throwable t) {
            defaultMaxOrder = 11;
        }
        // 5.对树的深度DEFAULT_MAX_ORDER进行初始化 默认值为11
        // 用户可以通过io.netty.allocator.maxOrder来进行设置
        DEFAULT_MAX_ORDER = defaultMaxOrder;

        // Determine reasonable default for nDirectArena.
        // Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
        final Runtime runtime = Runtime.getRuntime();
        // 6.初始化默认chunk的大小，为PageSize * (2的maxOrder幂)
        final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
        // 7.计算PoolAreana的个数 PoolArena默认为:cpu核心线程数与最大堆内存/2/(3*chunkSize)这两个数中的较小者
        // 这里的除以2是为了确保系统分配的所有PoolArena占用的内存不超过系统可用内存的一半，这里的除以3是为了保证每个PoolArena至少可以由3个PoolChunk组成
        // 用户可以通过io.netty.allocator.numHeapArenas／numDirectArenas来进行修改
        DEFAULT_NUM_DIRECT_ARENA = Math.max(0,
                SystemPropertyUtil.getInt(
                        "io.netty.allocator.numDirectArenas",
                        (int) Math.min(
                                runtime.availableProcessors(),
                                PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)));

    }

    public static final PooledByteBufAllocator DEFAULT = new PooledByteBufAllocator();

    private final PoolArena<ByteBuffer>[] directArenas;

    final ThreadLocal<PoolThreadCache> threadCache = new ThreadLocal<PoolThreadCache>() {
        private final AtomicInteger index = new AtomicInteger();
        @Override
        protected PoolThreadCache initialValue() {
            final int idx = index.getAndIncrement();
            final PoolArena<ByteBuffer> directArena;

            if (directArenas != null) {
                directArena = directArenas[Math.abs(idx % directArenas.length)];
            } else {
                directArena = null;
            }

            return new PoolThreadCache(directArena);
        }
    };

    public PooledByteBufAllocator() {
        this(DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER);
    }

    public PooledByteBufAllocator(int nDirectArena, int pageSize, int maxOrder) {

        // 1.chunkSize初始化 其值为pageSize*2^maxOrder
        final int chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        if (nDirectArena < 0) {
            throw new IllegalArgumentException("nDirectArea: " + nDirectArena + " (expected: >= 0)");
        }

        int pageShifts = validateAndCalculatePageShifts(pageSize);
        //创建PoolArena数组
        if (nDirectArena > 0) {
            directArenas = newArenaArray(nDirectArena);
            for (int i = 0; i < directArenas.length; i ++) {
                directArenas[i] = new PoolArena.DirectArena(this, pageSize, maxOrder, pageShifts, chunkSize);
            }
        } else {
            directArenas = null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> PoolArena<T>[] newArenaArray(int size) {
        return new PoolArena[size];
    }

    private static int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: 4096+)");
        }

        // Ensure pageSize is power of 2.
        boolean found1 = false;
        int pageShifts = 0;
        for (int i = pageSize; i != 0 ; i >>= 1) {
            if ((i & 1) != 0) {
                if (!found1) {
                    found1 = true;
                } else {
                    throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2");
                }
            } else {
                if (!found1) {
                    pageShifts ++;
                }
            }
        }
        return pageShifts;
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i --) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    @Override
    protected ByteBuf newDirectBuffer(int capacity) {
        PoolThreadCache cache = threadCache.get();
        PoolArena<ByteBuffer> directArena = cache.directArena;
        if (directArena != null) {
            return directArena.allocate(cache, capacity);
        } else {
            if (PlatformDependent.hasUnsafe()) {
                return new UnpooledUnsafeDirectByteBuf(this, capacity);
            } else {
                return new UnpooledDirectByteBuf(this, capacity);
            }
        }
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(directArenas.length);
        buf.append(" direct arena(s):");
        buf.append(StringUtil.NEWLINE);
        for (PoolArena<ByteBuffer> a: directArenas) {
            buf.append(a);
        }
        return buf.toString();
    }
}
