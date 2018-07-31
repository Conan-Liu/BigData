package com.conan.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by Administrator on 2017/7/6.
 * <p/>
 * 该类的功能： 第一列的值大于第二列则计数加 1
 */
public class GenericUDAFTest extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 2) {
            throw new UDFArgumentException("Exactly two argument is expected.");
        }
        return new GenericUDAFTestEvaluator();
    }

    public static class GenericUDAFTestEvaluator extends GenericUDAFEvaluator {

        private IntWritable result;
        private PrimitiveObjectInspector inputIO1;
        private PrimitiveObjectInspector inputIO2;

        /**
         * map 和 reduce阶段都需要执行
         * <p/>
         * map阶段： parameters 长度与udaf输入的参数个数有关, 即原始数据
         * reduce阶段： parameters长度为 1 , 即部分聚集数据, 就是上一个init的返回值
         * <p/>
         * 确定返回值
         * map阶段： 可以确定terminatePartial的返回值
         * reduce阶段： 可以确定terminate的返回值
         * 也就是说，这个init可以确定两个步骤的返回值
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            result = new IntWritable(0);
            inputIO1 = (PrimitiveObjectInspector) parameters[0];
            if (parameters.length > 1) {
                // map阶段parameters和udaf的参数个数有关， reduce阶段参数个数只有一个
                inputIO2 = (PrimitiveObjectInspector) parameters[1];
            }

            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        public static class TestAgg implements AggregationBuffer {
            // 计数，保存每次临时的结果
            int count;
        }

        // 获得一个聚合的缓冲对象，每个map执行一次
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            TestAgg agg = new TestAgg();
            reset(agg);
            return agg;
        }

        // 重置初始化
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            TestAgg myagg = (TestAgg) agg;
            myagg.count = 0;
        }

        // map端
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 2);

            if (parameters == null || parameters[0] == null || parameters[1] == null) {
                return;
            }

            double base = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputIO1);
            double tmp = PrimitiveObjectInspectorUtils.getDouble(parameters[1], inputIO2);

            if (base > tmp) {
                ((TestAgg) agg).count++;
            }
        }

        // 该方法当做iterate执行后，部分结果返回, map combine后返回
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            result.set(((TestAgg) agg).count);
            return result;
        }

        // reduce 端合并聚集结果
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                TestAgg myagg = (TestAgg) agg;
                long p = PrimitiveObjectInspectorUtils.getLong(partial, inputIO1);
                myagg.count += p;
            }
        }

        // reduce 或者只有map任务的job 输出结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            result.set(((TestAgg) agg).count);
            return result;
        }
    }
}
