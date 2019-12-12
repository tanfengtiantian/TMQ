package io.kafka.utils;


import io.kafka.mx.BrokerTopicStat;
import io.kafka.mx.IMBeanName;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Properties;
import java.util.zip.CRC32;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午10:12:35
 * @ClassName 工具类
 */
public class Utils {

	public static int getInt(Properties props, String name, int defaultValue) {
        return getIntInRange(props, name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public static int getInt(String name, int defaultValue) {
	    int dv = defaultValue;
        try {
            dv = Integer.parseInt(name);
        }catch (Exception e){

        }
        return dv;
    }
	
	public static int getIntInRange(Properties props, String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.getProperty(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }

    public static int getIntInRange(Map<String,String> props, String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.get(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }
	
	
	
	
	public static File getCanonicalFile(File f) {
        try {
            return f.getCanonicalFile();
        } catch (IOException e) {
            return f.getAbsoluteFile();
        }
    }
	
	public static String getString(Properties props, String name, String defaultValue) {
        return props.containsKey(name) ? props.getProperty(name) : defaultValue;
    }

    public static String[] getStrings(Properties props, String name, String defaultValue) {
        return props.containsKey(name) ? props.getProperty(name).split(",") : new String[0];
    }
	
	public static String getString(Properties props, String name) {
        if (props.containsKey(name)) {
            return props.getProperty(name);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }
	public static boolean getBoolean(Properties props, String name, boolean defaultValue) {
        if (!props.containsKey(name)) return defaultValue;
        return "true".equalsIgnoreCase(props.getProperty(name));
    }
	
	//********Channel************/
	@SuppressWarnings("resource")
	public static FileChannel openChannel(File file, boolean mutable) throws IOException {
        if (mutable) {
            return new RandomAccessFile(file, "rw").getChannel();
        }
        return new FileInputStream(file).getChannel();
    }


	public static String fromBytes(byte[] b) {
        return fromBytes(b, "UTF-8");
    }

    public static String fromBytes(byte[] b, String encoding) {
        if (b == null) return null;
        try {
            return new String(b, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b);
        }
    }


	public static byte[] getBytes(String s, String encoding) {
		if (s == null) return null;
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
	}


	public static long crc32(byte[] bytes) {
		return crc32(bytes, 0, bytes.length);
	}
	
	/**
     * Compute the CRC32 of the segment of the byte array given by the
     * specificed size and offset
     *
     * @param bytes  The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size   the number of bytes to checksum
     * @return The CRC32
     */
    public static long crc32(byte[] bytes, int offset, int size) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }


	public static void putUnsignedInt(ByteBuffer buffer, long value) {
		 buffer.putInt((int) (value & 0xffffffffL));
	}


	public static long getUnsignedInt(ByteBuffer buffer, int index) {
		return buffer.getInt(index) & 0xffffffffL;
	}


	public static String toString(ByteBuffer buffer, String encoding) {
		byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return fromBytes(bytes, encoding);
	}

    public static String toString(final byte[] bytes) {
        try {
            return new String(bytes,  "utf-8");
        }
        catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


	public static KV<String, Integer> getTopicPartition(String topicPartition) {
	     int index = topicPartition.lastIndexOf('-');
	        return new KV<String, Integer>(topicPartition.substring(0, index),//
	                Integer.valueOf(topicPartition.substring(index + 1)));
	}

	public static int read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
		int count = channel.read(buffer);
        if (count == -1) throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");
        return count;
	}

	public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        return thread;
	}

	

	public static int caculateShortString(String topic) {
		 return 2 + getBytes(topic).length;
	}
	
	public static byte[] getBytes(String s) {
        return getBytes(s, "UTF-8");
    }

	public static String readShortString(ByteBuffer buffer) {
		short size = buffer.getShort();
        if (size < 0) {
            return null;
        }
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return fromBytes(bytes);
	}
	
	public static void writeShortString(ByteBuffer buffer, String s) {
		if (s == null) {
            buffer.putShort((short) -1);
        } else if (s.length() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
        } else {
            byte[] data = getBytes(s); 
            buffer.putShort((short) data.length);
            buffer.put(data);
        }
	}

	public static Properties loadProps(String pName) {
		Properties props = new Properties();
		InputStreamReader isr = null;
        try {
        	isr = new InputStreamReader(Utils.class.getResourceAsStream("/"+pName));
            props.load(isr);
            return props;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(isr);
        }
	}

    public static Properties getResourceAsProperties(String resource, String encoding) throws IOException{
        InputStream in = null;
        try {
            in = ResourceUtils.getResourceAsStream(resource);
        }
        catch (IOException e) {
            File file = new File(resource);
            if (!file.exists()) {
                throw e;
            }
            in = new FileInputStream(file);
        }

        Reader reader = new InputStreamReader(in, encoding);
        Properties props = new Properties();
        props.load(reader);
        in.close();
        reader.close();

        return props;
    }

    /**
     * 注册到JMX
     * @param object
     * @return
     */
    public static boolean registerMBean(IMBeanName object) {
        return registerMBean(object, object.getMbeanName());
    }

    static boolean registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName)) {
                    mbs.unregisterMBean(objName);
                }
                mbs.registerMBean(mbean, objName);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        if (dir.isDirectory()) {
            File[] subs = dir.listFiles();
            if (subs != null) {
                for (File f : subs) {
                    deleteDirectory(f);
                }
            }
        }
        if (!dir.delete()) {
            throw new IllegalStateException("delete directory failed: " + dir);
        }
    }
}
