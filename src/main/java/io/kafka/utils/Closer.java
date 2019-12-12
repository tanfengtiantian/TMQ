package io.kafka.utils;


import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月17日 上午9:45:24
 * @ClassName 关闭某些流或文件描述的有用工具
 */
public class Closer {
	private static final Logger closerLogger = LoggerFactory.getLogger(Closer.class);

    public static void close(java.io.Closeable closeable) throws IOException {
        close(closeable, closerLogger);
    }

    public static void close(java.io.Closeable closeable, Logger logger) throws IOException {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    public static void closeQuietly(Selector selector) {
        closeQuietly(selector, closerLogger);
    }

    public static void closeQuietly(java.io.Closeable closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(java.nio.channels.Selector closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException e) {
            closerLogger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(ServerSocket serverSocket){
        if(serverSocket == null)return;
        try {
            serverSocket.close();
        } catch (IOException e) {
            closerLogger.error(e.getMessage(),e);
        }
    }
    /**
     * 悄悄地关闭一个可关闭对象(不抛出{IOException})
     * 
     */
    public static void closeQuietly(java.io.Closeable closeable) {
        closeQuietly(closeable, closerLogger);
    }
}
