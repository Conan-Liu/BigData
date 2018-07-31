package com.conan.source.sample;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * Created by Administrator on 2017/6/9.
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
