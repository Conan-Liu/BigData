package com.conan.bigdata.common.util;

import org.apache.log4j.Priority;
import org.apache.log4j.RollingFileAppender;

/**
 * 默认的log4j 打印日志输出的文件中低级别会包含高级别日志，
 * 比如定义了输出级别为info，则日志文件中会包含了：info以及比info高级别的warn,error等信息，造成文件的冗余，
 * 通过继承log4j的原始类，重写isAsSevereAsThreshold方法 只判断级别是否相等，不判断优先级
 */
public class MyRollingFileAppender extends RollingFileAppender {

    @Override
    public boolean isAsSevereAsThreshold(Priority priority) {
        // 只判断是否相等，而不判断优先级
        return this.getThreshold().equals(priority);
    }
}