package org.thomas.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


/**
 * Return a list of unique entries, for a given set of lists.
 * union: 去重
 * unionAll: 不去重
 * {1, 2} ∪ {1, 2} = {1, 2, 1, 2}
 * {1, 2} ∪ {2, 3} = {1, 2, 2 , 3}
 * {1, 2, 3} ∪ {3, 4, 5} = {1, 2, 3, 3, 4, 5}
 */

public class ArrayUnionALLUDF extends GenericUDF {
	// 用来处理数组类型数据 是 ListObjectInspector 的子类
	private StandardListObjectInspector retValInspector;

	private ListObjectInspector[] listInspectorArr;

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

		// udf array间遍历
		for (int i = 0; i < arg0.length; ++i) {
			Object undeferred = arg0[i].get();
			// udf内元素间遍历
			for (int j = 0; j < listInspectorArr[i].getListLength(undeferred); ++j) {
				Object nonStd = listInspectorArr[i].getListElement(undeferred, j);
				// o: 新元素 oi:
				InspectableObject stdInsp = new InspectableObject(nonStd, listInspectorArr[i].getListElementObjectInspector());
				objects.add(stdInsp);
			}
		}

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
		return "array_union_all(" + arg0[0] + ", " + arg0[1] + " )";
	}


	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if (arg0.length < 2) {
			throw new UDFArgumentException(" 需要两个 array类型的 参数 ");
		}
		ObjectInspector first = arg0[0];
		listInspectorArr = new ListObjectInspector[arg0.length];
		if (first.getCategory() == Category.LIST) {
			listInspectorArr[0] = (ListObjectInspector) first;
		} else {
			throw new UDFArgumentException(" 第一个参数需要为 array 类型 ");
		}
		// 支持多个参数
		for (int i = 1; i < arg0.length; ++i) {
			if (arg0[i].getCategory() != Category.LIST) {
				// udf的可变长参数 必须每个都是hive array类型
				throw new UDFArgumentException("需要array类型的列 index:" + i + " type: " + arg0[i].getCategory() );
			}
			// 强转hive array类型: ListObjectInspector
			ListObjectInspector checkInspector = (ListObjectInspector) arg0[i];
			if (!ObjectInspectorUtils.compareTypes(listInspectorArr[0].getListElementObjectInspector(), checkInspector.getListElementObjectInspector())) {
				// 校验数组泛型 即数组内元素类型都必须一致
				throw new UDFArgumentException(" Array types must match " + listInspectorArr[0].getTypeName() + " != " + checkInspector.getTypeName());
			}
			listInspectorArr[i] = checkInspector;
		}

		retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
		// 返回值的类型 就是输入的第一个array的 泛型
		return retValInspector;
	}

}
