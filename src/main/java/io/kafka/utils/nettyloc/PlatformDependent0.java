/*
 * Copyright 2013 The Netty Project
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The {@link PlatformDependent} operations which requires access to {@code sun.misc.*}.
 */
final class PlatformDependent0 {

    private static final Logger logger = LoggerFactory.getLogger(PlatformDependent0.class);

    private static final Unsafe UNSAFE;
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    private static final long CLEANER_FIELD_OFFSET;
    private static final long ADDRESS_FIELD_OFFSET;
    private static final Field CLEANER_FIELD;

    /**
     * {@code true} if and only if the platform supports unaligned access.
     *
     * @see <a href="http://en.wikipedia.org/wiki/Segmentation_fault#Bus_error">Wikipedia on segfault</a>
     */
    private static final boolean UNALIGNED;

    static {
        ByteBuffer direct = ByteBuffer.allocateDirect(1);
        Field cleanerField;
        try {
            cleanerField = direct.getClass().getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            Cleaner cleaner = (Cleaner) cleanerField.get(direct);
            cleaner.clean();
        } catch (Throwable t) {
            cleanerField = null;
        }
        CLEANER_FIELD = cleanerField;

        if (logger.isDebugEnabled()) {
            logger.debug("java.nio.ByteBuffer.cleaner: "+ (cleanerField != null? "available" : "unavailable"));
        }

        Field addressField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            if (addressField.getLong(ByteBuffer.allocate(1)) != 0) {
                addressField = null;
            } else {
                direct = ByteBuffer.allocateDirect(1);
                if (addressField.getLong(direct) == 0) {
                    addressField = null;
                }
                Cleaner cleaner = (Cleaner) cleanerField.get(direct);
                cleaner.clean();
            }
        } catch (Throwable t) {
            addressField = null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("java.nio.Buffer.address: "+ (addressField != null? "available" : "unavailable") );
        }

        Unsafe unsafe;
        if (addressField != null && cleanerField != null) {
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
                if (logger.isDebugEnabled()) {
                    logger.debug("sun.misc.Unsafe.theUnsafe: " + (unsafe != null? "available" : "unavailable") );
                }

                // Ensure the unsafe supports all necessary methods to work around the mistake in the latest OpenJDK.
                // https://github.com/netty/netty/issues/1061
                // http://www.mail-archive.com/jdk6-dev@openjdk.java.net/msg00698.html
                try {
                    unsafe.getClass().getDeclaredMethod(
                            "copyMemory",
                            new Class[] { Object.class, long.class, Object.class, long.class, long.class });

                    logger.debug("sun.misc.Unsafe.copyMemory: available");
                } catch (NoSuchMethodError t) {
                    logger.debug("sun.misc.Unsafe.copyMemory: unavailable");
                    throw t;
                } catch (NoSuchMethodException e) {
                    logger.debug("sun.misc.Unsafe.copyMemory: unavailable");
                    throw e;
                }
            } catch (Throwable cause) {
                unsafe = null;
            }
        } else {
            // If we cannot access the address of a direct buffer, there's no point of using unsafe.
            // Let's just pretend unsafe is unavailable for overall simplicity.
            unsafe = null;
        }
        UNSAFE = unsafe;

        if (unsafe == null) {
            CLEANER_FIELD_OFFSET = -1;
            ADDRESS_FIELD_OFFSET = -1;
            UNALIGNED = false;
        } else {
            ADDRESS_FIELD_OFFSET = objectFieldOffset(addressField);
            CLEANER_FIELD_OFFSET = objectFieldOffset(cleanerField);

            boolean unaligned;
            try {
                Class<?> bitsClass = Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
                Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
                unalignedMethod.setAccessible(true);
                unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
            } catch (Throwable t) {
                // We at least know x86 and x64 support unaligned access.
                String arch = SystemPropertyUtil.get("os.arch", "");
                //noinspection DynamicRegexReplaceableByCompiledPattern
                unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64)$");
            }

            UNALIGNED = unaligned;
            if (logger.isDebugEnabled()) {
                logger.debug("java.nio.Bits.unaligned: "+ UNALIGNED);
            }
        }
    }

    static boolean hasUnsafe() {
        return UNSAFE != null;
    }

    static void freeDirectBufferUnsafe(ByteBuffer buffer) {
        Cleaner cleaner;
        try {
            cleaner = (Cleaner) getObject(buffer, CLEANER_FIELD_OFFSET);
            if (cleaner == null) {
                throw new IllegalArgumentException(
                        "attempted to deallocate the buffer which was allocated via JNIEnv->NewDirectByteBuffer()");
            }
            cleaner.clean();
        } catch (Throwable t) {
            // Nothing we can do here.
        }
    }

    static void freeDirectBuffer(ByteBuffer buffer) {
        if (CLEANER_FIELD == null) {
            return;
        }
        try {
            Cleaner cleaner = (Cleaner) CLEANER_FIELD.get(buffer);
            if (cleaner == null) {
                throw new IllegalArgumentException(
                        "attempted to deallocate the buffer which was allocated via JNIEnv->NewDirectByteBuffer()");
            }
            cleaner.clean();
        } catch (Throwable t) {
            // Nothing we can do here.
        }
    }

    static long directBufferAddress(ByteBuffer buffer) {
        return getLong(buffer, ADDRESS_FIELD_OFFSET);
    }

    static long arrayBaseOffset() {
        return UNSAFE.arrayBaseOffset(byte[].class);
    }

    static Object getObject(Object object, long fieldOffset) {
        return UNSAFE.getObject(object, fieldOffset);
    }

    static int getInt(Object object, long fieldOffset) {
        return UNSAFE.getInt(object, fieldOffset);
    }

    private static long getLong(Object object, long fieldOffset) {
        return UNSAFE.getLong(object, fieldOffset);
    }

    static long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    static void copyMemory(long srcAddr, long dstAddr, long length) {
        UNSAFE.copyMemory(srcAddr, dstAddr, length);
    }

    static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
        UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, length);
    }

    private PlatformDependent0() {
    }
}
