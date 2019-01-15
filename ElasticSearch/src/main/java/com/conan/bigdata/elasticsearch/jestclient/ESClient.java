package com.conan.bigdata.elasticsearch.jestclient;

import com.google.gson.GsonBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.HttpHost;

/**
 * Created by Administrator on 2019/1/5.
 */
public class ESClient {
    private JestClient esClient;

    public JestClient getEsClient() {
        return this.esClient;
    }

    public ESClient(final String esURL) {
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig.Builder(esURL)
//                .setPreemptiveAuth(new HttpHost(esURL))
//                .defaultCredentials("","")
                .maxTotalConnection(10)
                .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create())
                .multiThreaded(true)
                .connTimeout(1000)
                .readTimeout(10000)
//                .discoveryEnabled(true)  // 加上这个参数报如下错: NoServerConfiguredException: No Server is assigned to client to connect
                ;

        factory.setHttpClientConfig(httpClientConfig.build());
        this.esClient = factory.getObject();
    }

    public void closeClient() {
        if (esClient != null) {
            esClient.shutdownClient();
        }
    }
}