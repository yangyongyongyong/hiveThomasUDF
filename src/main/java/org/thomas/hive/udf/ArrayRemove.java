package org.thomas.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import java.util.ArrayList;
import java.util.List;
// serde2 序列化 反序列化 库

/**
 * desc: 数组中移除单个元素
 *  select array_remove(`array`(11,22,33,22,99),22);
 *      [11,33,99]
 *   select array_remove(`array`("a","b","ccc","b"),"b");
 *      ["a","ccc"]
 *   select array_remove(`array`("a","b","ccc","b"),"b","a");
 *      ["ccc"]
 */

public class ArrayRemove extends GenericUDF {
    // 用来处理数组类型数据 是 ListObjectInspector 的子类
    private StandardListObjectInspector retValInspector;

    // 存储了udf的第一个参数  数组类型; 注意:这里并不会存储数据,只会存储udf输入的泛型. Array[T]
    private ListObjectInspector[] listInspectorArr;

    // 存储了udf第二个及之后所有元素 数组类型
    private PrimitiveObjectInspector[] objInspectorArr;

    // 这个内部类是为了比较 ObjectInspector 是否相同
    private class InspectableObject implements Comparable {
        public Object o; // 存储数据
        public ObjectInspector oi; // 存储数据的类型

        public InspectableObject(Object o, ObjectInspector oi) {
            this.o = o;
            this.oi = oi;
        }

        @Override
        public int hashCode() {
            return ObjectInspectorUtils.hashCode(o, oi);
        }

        @Override
        public int compareTo(Object arg0) {
            InspectableObject otherInsp = (InspectableObject) arg0;
            return ObjectInspectorUtils.compare(o, oi, otherInsp.o, otherInsp.oi);
        }

        @Override
        public boolean equals(Object other) {
            return compareTo(other) == 0;
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        // 这里treeSet是有序set 同时确保了数据的唯一性
        ArrayList<InspectableObject> objects = new ArrayList<InspectableObject>();

        Object undeferred = arg0[0].get();
        for (int j = 0; j < listInspectorArr[0].getListLength(undeferred); ++j) {
            Object nonStd = listInspectorArr[0].getListElement(undeferred, j);
            InspectableObject stdInsp = new InspectableObject(nonStd, listInspectorArr[0].getListElementObjectInspector());
            objects.add(stdInsp);
        }

        // udf array间遍历
        for (int i = 1; i < arg0.length; ++i) {
            // 需要剔除的单个元素
            undeferred = arg0[i].get();
            Object objectInspector = objInspectorArr[i].getPrimitiveWritableObject(undeferred);
            InspectableObject stdInsp = new InspectableObject(objectInspector, objInspectorArr[i]);
            while (objects.contains(stdInsp)) {
                objects.remove(stdInsp);
            }
        }

        // 返回值 retVal
        List retVal = (List) retValInspector.create(0);
        for (Object io : objects) {
            InspectableObject inspObj = (InspectableObject) io;
            // copyToStandardObject 根据 oi存储的类型进行转换 例如 oi是int类型 则返回Integer类型
            Object stdObj = ObjectInspectorUtils.copyToStandardObject(inspObj.o, inspObj.oi);
            retVal.add(stdObj);
        }
        return retVal;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "array_remove(" + arg0[0] + ", " + arg0[1] + " )";
    }


    // 初始化(把udf输入类型放入) listInspectorArr
    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        if (arg0.length < 2) {
            throw new UDFArgumentException(" 至少需要两个参数:  array<T>,T");
        }
        ObjectInspector first = arg0[0];
        listInspectorArr = new ListObjectInspector[1];
        objInspectorArr = new PrimitiveObjectInspector[arg0.length];

        // 参数1 数组泛型放入 listInspectorArr
        if (first.getCategory() == Category.LIST) {
            listInspectorArr[0] = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" 第一个参数需要为 array 类型 ");
        }

        // 支持多个参数
        for (int i = 1; i < arg0.length; ++i) {
            PrimitiveObjectInspector checkInspector2 = (PrimitiveObjectInspector) arg0[i];
            // 参数1(数组)元素的类型 需要等于 参数2+ 的数据类型.
            if (!ObjectInspectorUtils.compareTypes(listInspectorArr[0].getListElementObjectInspector(), checkInspector2)) {
                // 第一个为数组 第二个和之后的类型需要和数组内元素类型相同
                throw new UDFArgumentException("第二个参数及后续参数需要和第一个数组参数内元素类型一致");
            }
            objInspectorArr[i] = checkInspector2;
        }

        retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        // 返回值的类型 就是输入的第一个array的 泛型
        return retValInspector;
    }

}
