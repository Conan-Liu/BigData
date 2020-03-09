package com.conan.bigdata.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * 解析器和计算器。解析器负责UDAF的参数检查，操作符的重载以及对于给定的一组参数类型来查找正确的计算器
 */
public class GenericUDAFCollectList extends AbstractGenericUDAFResolver {

    // 解析器
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentException("Exactly one argument is expected.");
        }
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("Only primitive type arguments are accepted.");
        }
        return new GenericUDAFCollectListEvaluator();
    }


    // 计算器

    /**
     * PARTIAL1, //从原始数据到部分聚合数据的过程（map阶段），将调用iterate()和terminatePartial()方法。
     * PARTIAL2, //从部分聚合数据到部分聚合数据的过程（map端的combiner阶段），将调用merge() 和terminatePartial()方法。
     * FINAL,    //从部分聚合数据到全部聚合的过程（reduce阶段），将调用merge()和 terminate()方法。
     * COMPLETE  //从原始数据直接到全部聚合的过程（表示只有map，没有reduce，map端直接出结果），将调用merge() 和 terminate()方法。
     */
    /**
     * 通常还需要覆盖初始化方法ObjectInspector init(Mode m,ObjectInspector[] parameters)，
     * 需要注意的是，在不同的模式下parameters的含义是不同的，
     * 比如m为 PARTIAL1 和 COMPLETE 时，parameters为原始数据；
     * m为 PARTIAL2 和 FINAL 时，parameters仅为部分聚合数据（只有一个元素）。
     * 在 PARTIAL1 和 PARTIAL2 模式下，ObjectInspector  用于terminatePartial方法的返回值，
     * 在FINAL和COMPLETE模式下ObjectInspector 用于terminate方法的返回值。
     */
    public static class GenericUDAFCollectListEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputKeyOI;
        private StandardListObjectInspector lOI;
        private StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI));
            } else {
                if (parameters[0] instanceof StandardListObjectInspector) {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    lOI = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return lOI;
                } else {
                    inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI));
                }
            }
        }

        // 存储聚合结果
        static class CollectListAgg implements AggregationBuffer {
            List<Object> container = new ArrayList<Object>();
        }

        // 返回存储临时聚合结果的AggregationBuffer对象。
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CollectListAgg resultAgg = new CollectListAgg();
            return resultAgg;
        }

        // 重置聚合结果对象，以支持mapper和reducer的重用
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((CollectListAgg) agg).container.clear();
        }

        // 迭代处理原始数据parameters并保存到agg中
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null || parameters.length != 1) {
                return;
            }

            Object key = parameters[0];
            if (key != null) {
                CollectListAgg resultAgg = (CollectListAgg) agg;
                resultAgg.container.add(ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI));
            }
        }

        // 以持久化的方式返回agg表示的部分聚合结果，这里的持久化意味着返回值只能Java基础类型、数组、基础类型包装器、Hadoop的Writables、Lists和Maps
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            CollectListAgg resultAgg = (CollectListAgg) agg;
            List<Object> result = new ArrayList<Object>(resultAgg.container);
            return result;
        }

        // 合并由partial表示的部分聚合结果到agg中
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            CollectListAgg resultAgg = (CollectListAgg) agg;
            List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
            for (Object obj : partialResult) {
                resultAgg.container.add(ObjectInspectorUtils.copyToStandardObject(obj, this.inputKeyOI));
            }
        }

        // 返回最终结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            CollectListAgg resultAgg = (CollectListAgg) agg;
            Map<Text, Integer> map = new HashMap<Text, Integer>();
            int size = resultAgg.container.size();
            for (int i = 0; i < size; i++) {
                Text key = (Text) resultAgg.container.get(i);
                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + 1);
                } else {
                    map.put(key, 1);
                }
            }

            List<Map.Entry<Text, Integer>> listData = new ArrayList<Map.Entry<Text, Integer>>(map.entrySet());
            Collections.sort(listData, new Comparator<Map.Entry<Text, Integer>>() {
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    if (o1.getValue() < o2.getValue())
                        return 1;
                    else if (o1.getValue() == o2.getValue())
                        return 0;
                    else
                        return -1;
                }
            });

            List<Object> result = new ArrayList<Object>();
            for (Map.Entry<Text, Integer> entry : listData) {
                result.add(entry.getKey());
                result.add(new Text(entry.getValue().toString()));
            }

            return result;
        }
    }
}
