package com.conan.bigdata.flume.source.sample;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * 事件驱动
 * EventDrivenSource是需要触发一个调用机制，即被动等待
 * 例子
 * {@link org.apache.flume.source.AvroSource}
 * {@link org.apache.flume.source.ExecSource},
 * {@link org.apache.flume.source.SpoolDirectorySource}
 */
public class Source1 extends AbstractSource implements EventDrivenSource, Configurable {
    public void configure(Context context) {

    }

    @Override
    public synchronized void start() {

    }

    @Override
    public synchronized void stop() {

    }
}
