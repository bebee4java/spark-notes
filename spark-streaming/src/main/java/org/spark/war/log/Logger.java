package org.spark.war.log;


public abstract class Logger {
    public static Logger getInstance(String name) {
        return new LoggerImpl(name);
    }

    public static Logger getInstance(Class clazz) {
        return new LoggerImpl(clazz.getName());
    }

    public abstract String getName();

    public abstract boolean isDebugEnabled();

    public abstract boolean isInfoEnabled();

    public abstract boolean isWarnEnabled();

    public abstract boolean isErrorEnabled();

    public abstract boolean isTraceEnabled();

    public abstract void debug(String msg);

    public abstract void debug(String format, Object... args);

    public abstract void debug(String msg, Throwable t);

    public abstract void info(String msg);

    public abstract void info(String format, Object... args);

    public abstract void info(String msg, Throwable t);

    public abstract void warn(String msg);

    public abstract void warn(String format, Object... args);

    public abstract void warn(String msg, Throwable t);

    public abstract void error(String msg);

    public abstract void error(String format, Object... args);

    public abstract void error(String msg, Throwable t);

    public abstract void trace(String msg);

    public abstract void trace(String format, Object... args);

    public abstract void trace(String msg, Throwable t);

}
