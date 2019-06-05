package com.conan.bigdata.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2019/6/5.
 */
public class FilterWordInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(FilterWordInterceptor.class);

    @Override
    public void initialize() {
        LOG.warn("这是初始化的方法...................................");
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());
        try {
            Pattern pattern = Pattern.compile("^[0-9]*$");
            boolean isNumeric = pattern.matcher(body).matches();
            // 判断字符串是否为纯数字
            if (isNumeric) {
                headers.put("suffix", "numerics");
            } else {
                headers.put("suffix", "characters");
            }
        } catch (Exception e) {
            LOG.error("格式转换错误: [" + body + "]");
            return null;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new FilterWordInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}