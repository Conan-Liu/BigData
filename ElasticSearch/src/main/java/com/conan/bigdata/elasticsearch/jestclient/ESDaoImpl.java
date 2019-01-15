package com.conan.bigdata.elasticsearch.jestclient;

import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.indices.IndicesExists;
import io.searchbox.params.Parameters;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
            System.out.println("nodesInfo == " + result.getJsonString());
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
//                Object t1 = result.getSourceAsObject(Object.class);
//                LOG.info("getDocument == " + t1.toString());
//                System.out.println("getDocument == " + t1.toString());

                JSONObject r = result.getSourceAsObject(JSONObject.class);
                System.out.println("getDocument == " + r);
                System.out.println("module: " + r.getString("module"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult getAllDocument(String indices, String type) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(query).size(2);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indices).addType(type).build();
        SearchResult result = null;
        try {
            result = esClient.getEsClient().execute(search);
            System.out.println(result.getTotal());
            List<SearchResult.Hit<JSONObject, Void>> hits = result.getHits(JSONObject.class);
            System.out.println(hits.size());
            for (SearchResult.Hit<JSONObject, Void> hit : hits) {
                JSONObject jsonObject = hit.source;
                System.out.println(jsonObject.toJSONString());
                for (Map.Entry<String, Object> map : jsonObject.entrySet()) {
                    System.out.println(map.getValue().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult getDocumentFromScroll(String indices, String type) {
        String query = "{\"query\" : {\"match_all\" : { }}}";
        JestResult result = null;
        List<JSONObject> hits = null;
        String scrollId;
        try {
            // 循环拉取ES数据， 每一次拉50条， scroll的session context生存期是5分钟， scrollId是一样的
            Search search = new Search.Builder(query).addIndex(indices).addType(type).setParameter(Parameters.SIZE, 50).setParameter(Parameters.SCROLL, "5m").build();
            result = esClient.getEsClient().execute(search);
            scrollId = result.getJsonObject().get("_scroll_id").getAsString();
            System.out.println("scrollId: " + scrollId);
            hits = result.getSourceAsObjectList(JSONObject.class);
            do {
                for (JSONObject hit : hits) {
                    System.out.println(hit.toJSONString());
                }
                hits.clear();
                SearchScroll scroll = new SearchScroll.Builder(scrollId, "5m").build();
                result = esClient.getEsClient().execute(scroll);
                scrollId = result.getJsonObject().get("_scroll_id").getAsString();
                System.out.println("scrollId: " + scrollId);
                hits = result.getSourceAsObjectList(JSONObject.class);
            } while (hits.size() > 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        final String indices = "user_action-2019.01.14";
        final String type = "span";

        ESDaoImpl esDao = new ESDaoImpl("http://10.0.26.55:9200");

//        esDao.getDocument("user_action-2019.01.06", "span", "AWgnpUNQg4ixvVIcEDAU");

//        esDao.getAllDocument(indices, type);

//        esDao.nodesInof();

        esDao.getDocumentFromScroll(indices, type);
    }
}