package com.conan.crud;

/**
 * Created by Administrator on 2017/5/17.
 */
public interface IDocumentDao {

    String INDICE_NAME = "indexdb";
    String TYPE_NAME = "docs";

    boolean insert(Document doc);

    boolean replace(Document doc);

    boolean update(Document doc);

    boolean delete(long id);

    Document searchById(long id);
}
