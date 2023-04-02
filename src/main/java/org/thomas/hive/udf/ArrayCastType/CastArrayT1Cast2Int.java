package org.thomas.hive.udf.ArrayCastType;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 数据类型转换 array[String] 转换为 array[T2]  T1,T2 可以是 int,long,float,double,string
 */
public class CastArrayT1Cast2Int extends UDF {
    public List<Integer> evaluate(ArrayList<String> arr) {
        if (arr == null) {
            return null;
        }
        List<Integer> collect = arr.stream().map(Integer::parseInt).collect(Collectors.toList());

        return collect;
    }
}
