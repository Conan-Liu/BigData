package com.conan.bigdata.flume.source.sample;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * Created by Administrator on 2017/6/9.
 * <p>
 * 轮询拉取
 * PollableSource是通过线程不断去调用process方法，主动拉取消息
 * 例子：
 */
public class Source2 extends AbstractSource implements PollableSource, Configurable {
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
