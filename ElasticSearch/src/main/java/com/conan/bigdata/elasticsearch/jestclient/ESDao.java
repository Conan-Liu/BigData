package com.conan.bigdata.elasticsearch.jestclient;

import io.searchbox.client.JestResult;

/**
 * Created by Administrator on 2019/1/5.
 * ES操作 抽象方法 基本包含所有基本操作
 */
public interface ESDao {

    JestResult indicesExist();

    JestResult nodesInof();

    JestResult health();

    JestResult nodesStats();

    JestResult getDocument(String indices, String type, String id);

    JestResult getAllDocument(String indices, String type);

    JestResult getDocumentFromScroll(String indices, String type);
}