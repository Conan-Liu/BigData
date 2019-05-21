package com.conan.bigdata.flume.Interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2019/5/21.
 *
 * 这个是flume的拦截器， 对应的应用程序
 * com.conan.bigdata.spark.streaming.mwee.buriedpoint.DataCleaning
 * 这是埋点日志数据的拦截器
 */
public class CustomDynamicFileNameInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(CustomDynamicFileNameInterceptor.class);
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DEFAULT = true;

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        try {
            String body = new String(event.getBody());
            JSONObject jsonObj = JSONObject.parseObject(body);
            long timestamp = jsonObj.getLong("timestamp");
            String timePath = DateFormatUtils.format(new Date(timestamp), "yyyy/MM/dd");
            headers.put("timePath", timePath);
            LOG.debug("timePath=[" + timePath + "]");
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getStackTrace(e));
            return null;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> out = new ArrayList<>();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomDynamicFileNameInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}