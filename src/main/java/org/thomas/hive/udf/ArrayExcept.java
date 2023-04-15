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


/**
 * desc: 数组差集 保证第一个数组的元素顺序不变. 注意:该函数不会去做元素去重.
 *  select array_except(`array`(11,22,33),`array`(22));
 *      [11,33]
 *  select array_except(`array`(44,11,22,33,11,11),`array`(22),`array`(11,33,11));
 *      [44]
 *  select array_except(`array`(44,11,22,33,11,11),`array`(11));
 *      [44,22,33]
 *  select array_except(`array`(44,11,22,33,11,11,null),`array`(22),`array`(11,33,11));
 *      [44,null]
 *  select array_except(`array`(44,11,22,33,11,11,null),`array`(22),`array`(11,33,11,null));
 *      [44]
 *  select array_except(`array`(44,11,22,33,11,11),`array`(22),`array`(11,33,11,null));
 *      [44]
 *  select array_except(`array`(11,222,3,44,555,6666),`array`(3),`array`(0,555));
 *      [11,222,44,6666]
 *  select array_except(`array`("aa","bbb","c","dd","eee","ffff"),`array`("c"),`array`("xyz","eee"));
 *      ["aa","bbb","dd","ffff"]
 */
public class ArrayExcept extends GenericUDF {
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
				InspectableObject stdInsp = new InspectableObject(nonStd, listInspectorArr[i].getListElementObjectInspector());

				if (i == 0){
					//第一个都放到objects中
					objects.add(stdInsp);
				}else{
					// 其余的 都从objects中剔除
					while (objects.contains(stdInsp)){
						// 避免第一个数组存在重复值 导致无法剔除干净
						objects.remove(stdInsp);
					}
				}
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
		return "array_except(" + arg0[0] + ", " + arg0[1] + " )";
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
