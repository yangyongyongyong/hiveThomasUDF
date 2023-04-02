package org.thomas.hive.udf.ArrayCastType;

import org.apache.hadoop.hive.ql.exec.UDF;

public class CastArrayT1Cast2Long extends UDF {
    public java.util.List<Long> evaluate(java.util.ArrayList<String> arr) {
        if (arr == null) {
            return null;
        }
        java.util.List<Long> collect = arr.stream().map(Long::parseLong).collect(java.util.stream.Collectors.toList());

        return collect;
    }
}
