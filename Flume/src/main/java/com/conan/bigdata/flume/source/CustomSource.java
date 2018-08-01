package com.conan.bigdata.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * Created by Administrator on 2017/6/9.
 */
public class CustomSource extends AbstractSource implements Configurable, PollableSource {

    private String myProp;

    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");
        this.myProp = myProp;
    }

    public Status process() throws EventDeliveryException {
        Status status = null;

        // start transaction

        return null;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
