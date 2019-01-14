package com.conan.bigdata.elasticsearch.crud;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2017/5/17.
 */
public class ESClient {
    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9300;

    // 该链接方式是基于TCP的， 据说到ES 5.0之后， 支持不如 http 性能好
    private static TransportClient client;

    public static synchronized TransportClient getClient() {
        if (client == null) {
            build();
        }
        return client;
    }

    public static void close(TransportClient client) {
        if (client != null) {
            client.close();
        }
    }

    private static void build() {
        try {
            Settings settings = Settings.settingsBuilder().put("cluster.name", "aaa").build();
            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ES_HOST), ES_PORT));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
