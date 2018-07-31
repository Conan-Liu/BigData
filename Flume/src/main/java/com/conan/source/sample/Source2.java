package com.conan.source.sample;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * Created by Administrator on 2017/6/9.
 */
public class Source2 extends AbstractSource implements Configurable,PollableSource {
    public void configure(Context context) {

    }

    public Status process() throws EventDeliveryException {
        return null;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
