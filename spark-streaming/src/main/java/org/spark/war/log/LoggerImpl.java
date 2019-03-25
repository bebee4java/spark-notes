package org.spark.war.log;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.LoggerFactory;
import org.spark.war.common.CommonUtil;

import java.io.File;
import java.net.MalformedURLException;



class LoggerImpl extends Logger {
    static {
        File file = new File(CommonUtil.getConfDir(), "log4j.properties");
        if (file.exists() && file.isFile()) try {
            PropertyConfigurator.configure(file.toURI().toURL());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    private final org.slf4j.Logger logger;

    public LoggerImpl(String clazzname) {
        logger = LoggerFactory.getLogger(clazzname);
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void debug(String msg) {
        logger.debug(msg);
    }

    @Override
    public void debug(String format, Object... args) {
        logger.debug(format, args);
    }

    @Override
    public void debug(String msg, Throwable t) {
        logger.debug(msg, t);
    }

    @Override
    public void info(String msg) {
        logger.info(msg);
    }

    @Override
    public void info(String format, Object... args) {
        logger.info(format, args);
    }

    @Override
    public void info(String msg, Throwable t) {
        logger.info(msg, t);
    }

    @Override
    public void warn(String msg) {
        logger.warn(msg);
    }

    @Override
    public void warn(String format, Object... args) {
        logger.warn(format, args);
    }

    @Override
    public void warn(String msg, Throwable t) {
        logger.warn(msg, t);
    }

    @Override
    public void error(String msg) {
        logger.error(msg);
    }

    @Override
    public void error(String format, Object... args) {
        logger.error(format, args);
    }

    @Override
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }

    @Override
    public void trace(String msg) {
        logger.trace(msg);
    }

    @Override
    public void trace(String format, Object... args) {
        logger.trace(format, args);
    }

    @Override
    public void trace(String msg, Throwable t) {
        logger.trace(msg, t);
    }
}
