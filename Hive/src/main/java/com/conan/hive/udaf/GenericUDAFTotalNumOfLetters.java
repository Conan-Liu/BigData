package com.conan.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by Administrator on 2017/7/7.
 */
@Description(name = "letters", value = "_FUNC_(expr) - 返回该列中所有字符串的字符总数")
public class GenericUDAFTotalNumOfLetters extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentException("Exactly on argument is expected.");
        }

        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);

        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed");
        }

        return new GenericUDAFTotalNumOfLettersEvaluator();
    }

    public static class GenericUDAFTotalNumOfLettersEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;

        private IntWritable letterSum;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            letterSum = new IntWritable(0);

            // 执行map，  或者是只有map任务
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        static class LetterSumAgg implements AggregationBuffer {
            int sum = 0;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LetterSumAgg result = new LetterSumAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((LetterSumAgg) agg).sum = 0;
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                LetterSumAgg myagg = (LetterSumAgg) agg;
                Object p1 = inputOI.getPrimitiveJavaObject(parameters[0]);
                myagg.sum += String.valueOf(p1).length();
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            letterSum.set(myagg.sum);
            return letterSum;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                LetterSumAgg myagg = (LetterSumAgg) agg;
                // Object partialSum = integerOI.getPrimitiveJavaObject(partial);
                int partialSum = PrimitiveObjectInspectorUtils.getInt(partial, integerOI);
                myagg.sum += partialSum;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            letterSum.set(myagg.sum);
            return letterSum;
        }
    }
}
