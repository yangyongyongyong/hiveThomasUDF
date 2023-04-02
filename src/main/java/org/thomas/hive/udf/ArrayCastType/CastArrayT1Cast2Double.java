package org.thomas.hive.udf.ArrayCastType;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CastArrayT1Cast2Double extends UDF {
    public List<Double> evaluate(ArrayList<String> arr) {
        if (arr == null) {
            return null;
        }
        List<Double> collect = arr.stream().map(Double::parseDouble).collect(Collectors.toList());

        return collect;
    }
}
