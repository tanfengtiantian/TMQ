package io.kafka.utils.timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Runnable} that changes the current thread name and reverts it back
 * when its execution ends. To change the default thread names set by Netty, use
 * {@link #setThreadNameDeterminer(ThreadNameDeterminer)}.
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 *
 * @apiviz.landmark
 * @apiviz.has org.jboss.netty.util.ThreadNameDeterminer oneway - -
 *
 */
public class ThreadRenamingRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ThreadRenamingRunnable.class);
    private final Runnable runnable;
    private final String proposedThreadName;


    /**
     * Creates a new instance which wraps the specified {@code runnable} and
     * changes the thread name to the specified thread name when the specified
     * {@code runnable} is running.
     */
    public ThreadRenamingRunnable(Runnable runnable, String proposedThreadName) {
        if (runnable == null) {
            throw new NullPointerException("runnable");
        }
        if (proposedThreadName == null) {
            throw new NullPointerException("proposedThreadName");
        }
        this.runnable = runnable;
        this.proposedThreadName = proposedThreadName;
    }


    public void run() {
        final Thread currentThread = Thread.currentThread();
        final String oldThreadName = currentThread.getName();
        final String newThreadName = this.proposedThreadName;

        // Change the thread name before starting the actual runnable.
        boolean renamed = false;
        if (!oldThreadName.equals(newThreadName)) {
            try {
                currentThread.setName(newThreadName);
                renamed = true;
            }
            catch (SecurityException e) {
                logger.debug("Failed to rename a thread " + "due to security restriction.", e);
            }
        }

        // Run the actual runnable and revert the name back when it ends.
        try {
            this.runnable.run();
        }
        finally {
            if (renamed) {
                // Revert the name back if the current thread was renamed.
                // We do not check the exception here because we know it works.
                currentThread.setName(oldThreadName);
            }
        }
    }

}