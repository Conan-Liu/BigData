package com.conan.bigdata.flume.interceptor;

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
 * 这个是flume的拦截器， 对应的应用程序
 * com.conan.bigdata.spark.streaming.mwee.buriedpoint.DataCleaning
 * 这是埋点日志数据的拦截器
 *
 * 对应flume.conf
 * #配置agent1表示代理名称 ：
 * agent1.sources=source1
 * agent1.sinks=sink1
 * agent1.channels=channel1

 * #配置source1：
 * agent1.sources.source1.type = org.apache.flume.source.kafka.KafkaSource
 * agent1.sources.source1.kafka.bootstrap.servers=10.1.24.159:9092,10.1.24.160:9092,10.1.24.161:9092
 * agent1.sources.source1.kafka.consumer.auto.offset.reset=earliest
 * agent1.sources.source1.groupId =meimeng2hdfs_group
 * agent1.sources.source1.kafka.topics =MEIMENG_DATA_CLEANING_Topic_20190102
 * agent1.sources.source1.channels=channel1

 * #配置source1拦截器：
 * agent1.sources.source1.interceptors = i1
 * agent1.sources.source1.interceptors.i1.type = com.conan.bigdata.flume.interceptor.CustomDynamicFileNameInterceptor$Builder


 * #配置channel1：
 * agent1.channels = channel1
 * agent1.channels.channel1.type = file
 * agent1.channels.channel1.checkpointDir=/bdata/flume-1.8.0/data/meimeng/file-channel/checkpoint
 * agent1.channels.channel1.dataDirs=/bdata/flume-1.8.0/data/meimeng/file-channel/data

 * #配置sink1：
 * agent1.sinks.sink1.channel=channel1
 * agent1.sinks.sink1.type=hdfs
 * agent1.sinks.sink1.hdfs.path=hdfs://nameservice1/meimeng/activity/%{timePath}
 * agent1.sinks.sink1.hdfs.rollInterval=0
 * agent1.sinks.sink1.hdfs.filePrefix=meimeng
 * agent1.sinks.sink1.hdfs.inUsePrefix=.
 * agent1.sinks.sink1.hdfs.rollSize=134217728
 * agent1.sinks.sink1.hdfs.rollCount=0
 * agent1.sinks.sink1.hdfs.rollInterval=1800
 * agent1.sinks.sink1.hdfs.fileType=DataStream
 * agent1.sinks.sink1.hdfs.writeFormat=Text
 * agent1.sinks.sink1.hdfs.useLocalTimeStamp=true
 *
 * 数据存到hdfs上文件名样例， 上面的sink， 定义了文件大小和时间来切分文件， 128MB一个文件， 或半小时一个
 * /meimeng/activity/2019/05/13/meimeng.1558425412806
 *
 * 如果要发射自定定义的Event， 可以使用 {@link org.apache.flume.event.SimpleEvent}类
 */
public class CustomDynamicFileNameInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(CustomDynamicFileNameInterceptor.class);
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DEFAULT = true;

    /**
     * 该方法用来初始化拦截器，在拦截器的构造方法执行之后执行，也就是创建完拦截器对象之后执行
     */
    @Override
    public void initialize() {

    }

    /**
     * 用来处理每一个event对象，该方法不会被系统自动调用，一般在 List<Event> intercept(List<Event> events) 方法内部调用。
     * @param event
     * @return
     */
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

    /**
     * 用来处理一批event对象集合，集合大小与flume启动配置有关，和transactionCapacity大小保持一致。一般直接调用 Event intercept(Event event) 处理每一个event数据。
     * @param events
     * @return
     */
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