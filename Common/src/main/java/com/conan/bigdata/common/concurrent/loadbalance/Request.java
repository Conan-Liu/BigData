package com.conan.bigdata.common.concurrent.loadbalance;

import java.io.InputStream;

/**
 * 具体的请求实例
 */
public class Request {

    private final long requestId;
    private final int requestType;
    private InputStream in;

    public Request(long requestId, int requestType) {
        this.requestId = requestId;
        this.requestType = requestType;
    }

    public long getRequestId() {
        return this.requestId;
    }

    public int getRequestType() {
        return this.requestType;
    }

    public InputStream getIn() {
        return this.in;
    }

    public void setIn(InputStream in) {
        this.in = in;
    }
}
