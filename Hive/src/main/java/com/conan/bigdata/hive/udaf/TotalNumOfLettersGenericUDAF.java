package com.conan.bigdata.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * 主要作用是实现参数类型检查和操作符重载。可以为同一个函数实现不同入参的版本
 */
public class TotalNumOfLettersGenericUDAF extends AbstractGenericUDAFResolver {

    private static final Log LOG = LogFactory.getLog(TotalNumOfLettersGenericUDAF.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return super.getEvaluator(info);
    }

    public static class GenericUDAFMeberLevelEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector inputOI2;
        private PrimitiveObjectInspector outputOI;
        private DoubleWritable result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                inputOI2 = (PrimitiveObjectInspector) parameters[1];
                result = new DoubleWritable(0);
            }

            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                outputOI = (PrimitiveObjectInspector) parameters[0];
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            } else {
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        static class SumAgg implements AggregationBuffer {
            boolean empty;
            double value;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumAgg buffer = new SumAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((SumAgg) agg).value = 0.0;
            ((SumAgg) agg).empty = true;
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null) {
                return;
            }
            try {
                double flag = PrimitiveObjectInspectorUtils.getDouble(parameters[1], inputOI2);
                if (flag > 1.0)
                    merge(agg, parameters[0]);
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + "\t" + StringUtils.stringifyException(e));
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                if (inputOI != null) {
                    double p = PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
                    ((SumAgg) agg).value += p;
                } else {
                    double p = PrimitiveObjectInspectorUtils.getDouble(partial, outputOI);
                    ((SumAgg) agg).value += p;
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumAgg myagg = (SumAgg) agg;
            result.set(myagg.value);
            return result;
        }
    }
}
