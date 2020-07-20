package com.conan.bigdata.hive.thrift;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO ...
public class HiveThriftExp {

    private static final String HOST = "hadoopmac";
    private static final int PORT = 10000;

    public static void main(String[] args) throws Exception {
        HiveThriftExp thriftExp = new HiveThriftExp();
        thriftExp.submitQuery(" desc test1;");
    }

    public TCLIService.Client getClient() throws Exception {
        TTransport transport = HiveAuthFactory.getSocketTransport(HOST, PORT, 9999);
        // 创建通信协议，和服务端的协议一致
        TProtocol protocol = new TBinaryProtocol(transport);
        TCLIService.Client client = new TCLIService.Client(protocol);
        transport.open();
        return client;
    }

    public TOperationHandle submitQuery(String command) throws Exception {

        TCLIService.Client client = getClient();
        TOpenSessionReq sessionReq = new TOpenSessionReq();
        TOpenSessionResp sessionResp = client.OpenSession(sessionReq);
        // client.send_OpenSession(sessionReq);
        // TOpenSessionResp sessionResp = client.recv_OpenSession();
        TSessionHandle sessionHandle = sessionResp.getSessionHandle();
        TExecuteStatementReq statementReq = new TExecuteStatementReq(sessionHandle, command);

        statementReq.setRunAsync(true);
        // 执行语句
        TExecuteStatementResp statementResp = client.ExecuteStatement(statementReq);
        // 获取执行Handler
        TOperationHandle operationHandle = statementResp.getOperationHandle();
        if (operationHandle == null) {
            // 语句运行异常
            throw new Exception(statementResp.getStatus().getErrorMessage());
        }

        getResults(operationHandle);
        return null;
    }

    public List<Object> getResults(TOperationHandle operationHandle) {
        TFetchResultsReq resultsReq = new TFetchResultsReq();
        resultsReq.setOperationHandle(operationHandle);
        resultsReq.setMaxRows(100);

        TFetchResultsResp resultsResp = new TFetchResultsResp();
        List<TColumn> columns = resultsResp.getResults().getColumns();
        List<Object> list_row = new ArrayList<>();
        for (TColumn field : columns) {
            if (field.isSetStringVal()) {
                list_row.add(field.getStringVal().getValues());
            } else if (field.isSetDoubleVal()) {
                list_row.add(field.getDoubleVal().getValues());
            } else if (field.isSetI16Val()) {
                list_row.add(field.getI16Val().getValues());
            } else if (field.isSetI32Val()) {
                list_row.add(field.getI32Val().getValues());
            } else if (field.isSetI64Val()) {
                list_row.add(field.getI64Val().getValues());
            } else if (field.isSetBoolVal()) {
                list_row.add(field.getBoolVal().getValues());
            } else if (field.isSetByteVal()) {
                list_row.add(field.getByteVal().getValues());
            }
        }

        for (Object obj : list_row) {
            System.out.println(obj);
        }

        return list_row;
    }

    /**
     * 考虑kerberos认证
     */
    public TTransport getTransport() {
        TTransport transport = HiveAuthFactory.getSocketTransport(HOST, PORT, 9999);
        // 是否启用安全认证
        boolean isSecurityEnable = UserGroupInformation.isSecurityEnabled();
        if (isSecurityEnable) {
            Map<String, String> saslProperties = new HashMap<>();
            saslProperties.put("javax.security.sasl.qop", "auth");//kerberos 认证关键参数
            saslProperties.put("javax.security.sasl.server.authentication", "true");//kerberos 认证关键参数
        }

        return transport;
    }
}
