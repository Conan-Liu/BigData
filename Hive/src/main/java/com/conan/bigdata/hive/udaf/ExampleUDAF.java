package com.conan.bigdata.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class ExampleUDAF implements GenericUDAFResolver2 {
    /**
     * 判断输入参数类型和定义处理器
     */
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // 不推荐使用
        TypeInfo[] parameters = info.getParameters();
        // 推荐，获取函数输入参数
        ObjectInspector[] parameterObjectInspectors = info.getParameterObjectInspectors();


        if (parameterObjectInspectors.length == 0) {
            if (!info.isAllColumns()) {
                throw new UDFArgumentException("Argument expected");
            }
        } else if (ObjectInspector.Category.PRIMITIVE != parameterObjectInspectors[0].getCategory()) {
            // 判断输入是否基本数据类型，并获取基本数据类型名称
            PrimitiveObjectInspector parameterObjectInspector = (PrimitiveObjectInspector) parameterObjectInspectors[0];
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = parameterObjectInspector.getPrimitiveCategory();
        } else {
            if (parameterObjectInspectors.length > 1 && !info.isDistinct()) {
                throw new UDFArgumentException("DISTINCT key must be specified");
            }
        }

        return new GenericUDAFExampleEvaluator();
    }

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new GenericUDAFExampleEvaluator();
    }

    public static class GenericUDAFExampleEvaluator extends GenericUDAFEvaluator {

        private boolean countAllColumns = false;
        private LongObjectInspector partialCountAggOI;
        private LongWritable result;

        // 初始化一个UDAF evaluator类
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            partialCountAggOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            result = new LongWritable(0);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        private GenericUDAFExampleEvaluator setCountAllColumns(boolean countAllCols) {
            countAllColumns = countAllCols;
            return this;
        }

        /**
         * AggregationBuffer 不再推荐使用
         *
         * 用于存储中间聚合结果
         */
        static class ExampleAgg extends AbstractAggregationBuffer {
            long value;

            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES2;
            }
        }

        // 获取一个存储中间结果的对象
        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            ExampleAgg buffer = new ExampleAgg();
            reset(buffer);
            return buffer;
        }

        // 重置对象
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ExampleAgg) agg).value = 0;
        }

        // map端将一行新的数据载入到聚合buffer中，迭代计算
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null) {
                return;
            }
            if (countAllColumns) {
                ((ExampleAgg) agg).value++;
            } else {
                boolean countThisRow = true;
                for (Object nextParam : parameters) {
                    if (nextParam == null) {
                        countThisRow = false;
                        break;
                    }
                }
                if (countThisRow) {
                    ((ExampleAgg) agg).value++;
                }
            }
        }

        // map端聚合
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        // reduce端合并聚合结果
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                long p = partialCountAggOI.get(partial);
                ((ExampleAgg) agg).value += p;
            }
        }

        // reduce端最终聚合
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            result.set(((ExampleAgg) agg).value);
            return result;
        }
    }
}
