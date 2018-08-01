package com.conan.bigdata.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Administrator on 2017/5/23.
 */
@Description(name = "median", value = "_FUNC_(x) return the median number of a number array. eg:median(x)")
public class GenericUDAFMedian extends AbstractGenericUDAFResolver {

    private static final Log LOG = LogFactory.getLog(GenericUDAFMedian.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentException("Only 1 parameter is accepted !");
        }

        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("Only primitive type arguments are accepted but "
                    + info[0].getTypeName() + " is passed."
            );
        }
//        ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);
//        if (!ObjectInspectorUtils.compareSupported(objectInspector)) {
//            throw new UDFArgumentException("Cannot support comparison of map<> type or complex type containing map<>.");
//        }

        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return new GenericUDAFMedianEvaluatorDouble();
            case STRING:
            case BOOLEAN:
            default:
                throw new UDFArgumentException("Only numeric type(int long double) arguments are accepted but" +
                        info[0].getTypeName() + " was passed as parameter of index -> 1.");
        }
    }

    public static class GenericUDAFMedianEvaluatorDouble extends GenericUDAFEvaluator {

        private DoubleWritable result;
        PrimitiveObjectInspector inputOI;
        StructObjectInspector structOI;
        StandardListObjectInspector listOI;
        StructField listField;
        Object[] partialResult;
        ListObjectInspector listFieldOI;

        /*
        PARTIAL1   mapper
        PARTIAL2   combine
        FINAL      reduce
        COMPLETE   只有mapper
         */

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            result = new DoubleWritable();
            listOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

            // init input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                structOI = (StructObjectInspector) parameters[0];
                listField = structOI.getStructFieldRef("list");
                listFieldOI = (ListObjectInspector) listField.getFieldObjectInspector();
            }

            // init output
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(listOI);
                List<String> fname = new ArrayList<String>();
                fname.add("list");
                partialResult = new Object[1];
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                // 定义该UDAF的返回类型 , 返回类型为 Double
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        // 存储计算中位数的数据记录
        static class MedianNumberAgg implements AggregationBuffer {
            List<DoubleWritable> aggIntegerList;
        }

        // 创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MedianNumberAgg resultAgg = new MedianNumberAgg();
            reset(resultAgg);
            return resultAgg;
        }

        // mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。就是多申请一块内存
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
            medianNumberAgg.aggIntegerList = null;
            medianNumberAgg.aggIntegerList = new ArrayList<DoubleWritable>();
        }

        boolean warned = false;

        // map阶段调用，只要把保存当前和的对象resultAgg，再加上输入的参数，ok
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
                double val = 0.0;
                try {
                    val = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
                } catch (NullPointerException e) {
                    LOG.warn("got a null value, skip it");
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                        LOG.warn("ignore similar exceptions.");
                    }
                }
                medianNumberAgg.aggIntegerList.add(new DoubleWritable(val));
            }
        }

        // mapper结束要返回的结果， 还有combiner结束的返回结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
            partialResult[0] = new ArrayList<DoubleWritable>(medianNumberAgg.aggIntegerList.size());
            ((List<DoubleWritable>) partialResult[0]).addAll(medianNumberAgg.aggIntegerList);

            return partialResult;
        }

        // combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
            Object partialObject = structOI.getStructFieldData(partial, listField);
            List<DoubleWritable> resultList = (List<DoubleWritable>) listFieldOI.getList(partialObject);
            for (DoubleWritable i : resultList) {
                medianNumberAgg.aggIntegerList.add(i);
            }
        }

        // reducer返回结果，或者只有mapper，没有reducer时，在mapper端返回结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MedianNumberAgg medianNumberAgg = (MedianNumberAgg) agg;
            Collections.sort(medianNumberAgg.aggIntegerList);
            int size = medianNumberAgg.aggIntegerList.size();
            if (size == 1) {
                result.set((double) medianNumberAgg.aggIntegerList.get(0).get());
                return result;
            }
            double rs = 0.0;
            int midIndex = size / 2;
            if (size % 2 == 1) {
                rs = (double) medianNumberAgg.aggIntegerList.get(midIndex).get();
            } else if (size % 2 == 0) {
                rs = (medianNumberAgg.aggIntegerList.get(midIndex - 1).get() + medianNumberAgg.aggIntegerList.get(midIndex).get()) / 2.0;
            }

            result.set(rs);
            return result;
        }
    }
}
