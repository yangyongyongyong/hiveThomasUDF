package org.thomas.hive.udf;



import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class ArrayPosition extends GenericUDF {

    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;
    private static final int ARG_COUNT = 2; // Number of arguments to this UDF
    private static final String FUNC_NAME = "ARRAY_INDEX_OF"; // External Name

    private ObjectInspector valueOI;
    private ListObjectInspector arrayOI;
    private ObjectInspector arrayElementOI;
    private IntWritable result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        // Check if two arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException("The function " + FUNC_NAME + " accepts " + ARG_COUNT + " arguments.");
        }

        // Check if ARRAY_IDX argument is of category LIST
        if (!arguments[ARRAY_IDX].getCategory().equals(Category.LIST)) {
            throw new UDFArgumentTypeException(ARRAY_IDX, "\"" + "LIST"
                    + "\" " + "expected at function " + FUNC_NAME + ", but " + "\"" + arguments[ARRAY_IDX].getTypeName()
                    + "\" " + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
        arrayElementOI = arrayOI.getListElementObjectInspector();

        valueOI = arguments[VALUE_IDX];

        // Check if list element and value are of same type
        if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
            throw new UDFArgumentTypeException(VALUE_IDX, "\"" + arrayElementOI.getTypeName() + "\""
                    + " expected at function " + FUNC_NAME + ", but " + "\"" + valueOI.getTypeName() + "\"" + " is found");
        }

        // Check if the comparison is supported for this type
        if (!ObjectInspectorUtils.compareSupported(valueOI)) {
            throw new UDFArgumentException("The function " + FUNC_NAME + " does not support comparison for " + "\""
                    + valueOI.getTypeName() + "\"" + " types");
        }

        result = new IntWritable(-1);

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        result.set(-1);

        Object array = arguments[ARRAY_IDX].get();
        Object value = arguments[VALUE_IDX].get();

        int arrayLength = arrayOI.getListLength(array);

        // Check if array is null or empty or value is null
        if (value == null || arrayLength <= 0) {
            return result;
        }

        // Compare the value to each element of array until a match is found
        for (int i = 0; i < arrayLength; ++i) {
            Object listElement = arrayOI.getListElement(array, i);
            if (listElement != null) {
                if (ObjectInspectorUtils.compare(value, valueOI, listElement, arrayElementOI) == 0) {
                    // 这里和spark同名函数保持一致逻辑  返回的index从1开始
                    result.set(i+1);
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == ARG_COUNT);
        return "array_position(" + children[ARRAY_IDX] + ", " + children[VALUE_IDX] + ")";
    }
}
