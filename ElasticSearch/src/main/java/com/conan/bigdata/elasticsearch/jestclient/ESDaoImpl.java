package com.conan.bigdata.elasticsearch.jestclient;

import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.Get;
import io.searchbox.indices.IndicesExists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created by Administrator on 2019/1/5.
 */
public class ESDaoImpl implements ESDao {

    private final static Logger LOG = LoggerFactory.getLogger(ESDaoImpl.class);

    private ESClient esClient;

    public ESDaoImpl(String esURL) {
        this.esClient = new ESClient(esURL);
    }

    @Override
    public JestResult indicesExist() {
        IndicesExists indicesExists = new IndicesExists.Builder("indices").build();
        JestResult result = null;
        try {
            result = esClient.getEsClient().execute(indicesExists);
            LOG.info("indicesExist == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult nodesInof() {
        NodesInfo nodesInfo = new NodesInfo.Builder().build();
        JestResult result = null;
        try {
            result = esClient.getEsClient().execute(nodesInfo);
            LOG.info("nodesInfo == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult health() {
        Health health = new Health.Builder().build();
        JestResult result = null;
        try {
            result = esClient.getEsClient().execute(health);
            LOG.info("health == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult nodesStats() {
        NodesStats nodesStats = new NodesStats.Builder().build();
        JestResult result = null;
        try {
            result = esClient.getEsClient().execute(nodesStats);
            LOG.info("nodesStats == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult getDocument(String indices, String type, String id) {
        Get get = new Get.Builder(indices, id).type(type).build();
        JestResult result = null;
        try {
            result = esClient.getEsClient().execute(get);
            if (result.isSucceeded()) {
                Object t1 = result.getSourceAsObject(Object.class);
                LOG.info("getDocument == " + t1.toString());
                System.out.println("getDocument == " + t1.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        ESDaoImpl esDao = new ESDaoImpl("http://10.0.26.55:9200");

        esDao.getDocument("user_action-2019.01.05", "span", "AWgdJ1Ldg4ixvVIcDisH");
    }
}