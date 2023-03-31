package org.thomas.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive udf 函数
 * 用来获取string类型 jsonArray的字符串列的长度
 * create temporary function size_json_array as "org.thomas.hive.udf.JsonArraySize";
 * select size_json_array('[{"k1":"v1"},{"k2":"v2"}]') ;
 */
public class JsonArraySize extends UDF {
    public int evaluate(String jsonArr) {
        if (jsonArr == null) {
            return 0;
        }
        return jsonArr.length();
    }
}
