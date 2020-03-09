package com.conan.bigdata.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 用户Source读取events发送到Sink的时候，在events header中加入一些有用的信息，或者对events的内容进行过滤，完成初步的数据清洗。
 */
public class FilterWordInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(FilterWordInterceptor.class);

    public FilterWordInterceptor() {
        LOG.warn("这是构造的方法...................................");
    }

    @Override
    public void initialize() {
        LOG.warn("这是初始化的方法...................................");
    }

    /**
     * 如果不想要的Event 返回null就好
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        LOG.warn("这是intercept 单个Event的方法...................................");
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());
        LOG.warn("=================" + body);
        try {
            boolean isNumeric = isNumeric(body);
            LOG.warn("=================" + isNumeric);
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
        LOG.warn("suffix**************" + event.getHeaders().get("suffix"));
        return event;
    }

    private boolean isNumeric(String body) {
        Pattern pattern = Pattern.compile("^[0-9]*$");
        return pattern.matcher(body).matches();
    }

//    @Override
//    public List<Event> intercept(List<Event> events) {
//        LOG.warn("这是intercept 多个Event的方法...................................");
//        List<Event> list = new ArrayList<>(events.size());
//        for (Event event : events) {
////            intercept(event);
//            Event e1 = intercept(event);
//            // 如果为null，就不输出了， 起到过滤作用
//            if (e1 != null) {
//                list.add(e1);
//            }
//            LOG.warn("**************" + e1.getHeaders().get("suffix"));
//        }
//        LOG.warn("===============" + list.size());
//        return list;
//    }

    /**
     * List是引用传递， 所以，直接修改值即可， 无需重新定义新的集合
     * 这个方法不知道什么情况下才能执行？
     * TODO...
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        LOG.warn("这是intercept 多个Event的方法...................................");
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        LOG.warn("这是close的方法...................................");
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            LOG.warn("这是build的方法...................................");
            return new FilterWordInterceptor();
        }

        @Override
        public void configure(Context context) {
            LOG.warn("configure...................................");
            LOG.info("param ==== " + context.getString("param"));
        }
    }
}