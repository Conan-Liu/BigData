package com.conan.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = false, stateful = true)
public class GenericUDFUniqueID extends GenericUDF {

    private static Text result;
    private static final char SEPARATOR = '_';
    private static final String ATTEMPT = "attempt";
    private static String clusterTimeStamp;
    private static String jobID;
    private int initID = 0;
    private int increment = 0;
    private String map_id;

    @Override
    public void configure(MapredContext context) {
        increment = context.getJobConf().getNumMapTasks();
        if (increment == 0) {
            throw new IllegalArgumentException("there is no map task.");
        }
        map_id = context.getJobConf().get("mapreduce.task.attempt.id", "NULL");
        if ("NULL".equals(map_id)) {
            throw new IllegalArgumentException("not get task id.");
        }
        initID = getInitID(map_id, increment);
        if (initID == 0l) {
            throw new IllegalArgumentException("mapreduce.task.attempt.id");
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        result = new Text();
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.set(getValue());
        increment(increment);
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "RowSeq-func()";
    }

    private synchronized void increment(int incr) {
        //initID += incr;
        initID += 1;
    }

    private synchronized String getValue() {
        return clusterTimeStamp + "-" + jobID + "-" + String.valueOf(initID);
    }

    private int getInitID(String taskAttemptIDstr, int numTasks) throws IllegalArgumentException {
        try {
            String[] parts = taskAttemptIDstr.split(Character.toString(SEPARATOR));
            if (parts.length == 6) {
                if (ATTEMPT.equals(parts[0])) {
                    if (!"m".equals(parts[3]) && !"r".equals(parts[3])) {
                        throw new Exception();
                    }
                    clusterTimeStamp = parts[1];
                    jobID = parts[2];
                    int result = Integer.parseInt(parts[4]);
                    if (result >= numTasks) {
                        throw new Exception("TaskAttemptId String : " + taskAttemptIDstr + " parse ID [" + result + "] >= numTasks[" + numTasks + "]...");
                    }
                    return result + 1;
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("TaskAttemptId string : " + taskAttemptIDstr + " is not properly formed");
        }
        return 0;
    }
}
