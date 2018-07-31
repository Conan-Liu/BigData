package com.conan.crud;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;

/**
 * Created by Administrator on 2017/5/17.
 */
public class DocumentDaoImpl implements IDocumentDao {

    // TCP连接客户端
    private TransportClient client = null;

    public DocumentDaoImpl(TransportClient client) {
        this.client = client;
    }

    @Override
    public boolean insert(Document doc) {
        String docJson = ToJSON.documentToJson(doc);
        IndexResponse response = client.prepareIndex(INDICE_NAME, TYPE_NAME, String.valueOf(doc.getId())).setSource(docJson).get();
        return response.isCreated();
    }

    @Override
    public boolean replace(Document doc) {
        return false;
    }

    @Override
    public boolean update(Document doc) {
        String docJson = ToJSON.documentToJson(doc);
        UpdateResponse response = client.prepareUpdate(INDICE_NAME, TYPE_NAME, String.valueOf(doc.getId())).setDoc(docJson).get();
        return !response.isCreated();
    }

    @Override
    public boolean delete(long id) {
        DeleteResponse response = client.prepareDelete(INDICE_NAME, TYPE_NAME, String.valueOf(id)).get();
        return response.isFound();
    }

    @Override
    public Document searchById(long id) {
        GetResponse response = client.prepareGet(INDICE_NAME, TYPE_NAME, String.valueOf(id)).get();
        if (response.isExists()) {
            String json = response.getSourceAsString();
            return ToJSON.jsonToDocument(json);
        } else {
            return null;
        }
    }
}
