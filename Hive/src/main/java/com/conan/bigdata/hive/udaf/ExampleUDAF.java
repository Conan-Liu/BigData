package com.conan.bigdata.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class ExampleUDAF implements GenericUDAFResolver2 {
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        TypeInfo[] parameters = info.getParameters();

        if (parameters.length == 0) {
            if (!info.isAllColumns()) {
                throw new UDFArgumentException("Argument expected");
            }
        } else {
            if (parameters.length > 1 && !info.isDistinct()) {
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

        static class ExampleAgg extends AbstractAggregationBuffer {
            long value;

            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES2;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ExampleAgg buffer = new ExampleAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ExampleAgg) agg).value = 0;
        }

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

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                long p = partialCountAggOI.get(partial);
                ((ExampleAgg) agg).value += p;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            result.set(((ExampleAgg) agg).value);
            return result;
        }
    }
}
