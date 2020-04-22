package io.kafka.utils;

public abstract class ExceptionMonitor {
    private static ExceptionMonitor instance = new DefaultExceptionMonitor();


    /**
     * Returns the current exception monitor.
     */
    public static ExceptionMonitor getInstance() {
        return instance;
    }


    /**
     * Sets the uncaught exception monitor. If <code>null</code> is specified,
     * the default monitor will be set.
     *
     * @param monitor
     *            A new instance of {@link DefaultExceptionMonitor} is set if
     *            <tt>null</tt> is specified.
     */
    public static void setInstance(ExceptionMonitor monitor) {
        if (monitor == null) {
            monitor = new DefaultExceptionMonitor();
        }
        instance = monitor;
    }


    /**
     * Invoked when there are any uncaught exceptions.
     */
    public abstract void exceptionCaught(Throwable cause);
}