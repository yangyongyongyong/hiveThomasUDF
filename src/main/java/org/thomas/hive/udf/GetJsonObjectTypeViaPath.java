package org.thomas.hive.udf;

import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.math.BigDecimal;


/**
 * 作用: 获取json中某个key对应的value的数据类型(用来识别json value类型是否符合预期). 只支持区分long和string
 * 解决痛点: hive自带的get_json_object无法识别数据类型,都按string处理
 *
 * 测试:
 *              Object obj = null;
 *         obj = JSONPath.of("$.k").extract(JSONReader.of("{\"k\":\"100\"}"));
 *         System.out.println(obj instanceof Integer); // "{\"k\":100}"
 *         System.out.println(obj instanceof Long);    // "{\"k\":100000000000}"
 *         System.out.println(obj instanceof BigDecimal); // "{\"k\":22.22}"
 *         System.out.println(obj instanceof String); // "{\"k\":\"100\"}"
 *         System.out.println(obj.getClass());
 */
public class GetJsonObjectTypeViaPath extends UDF {
    public String evaluate(String json, String path) {
        if (json == null || path == null) {
            return null;
        }
        Object obj = null;
        try{
            obj = JSONPath.of(path).extract(JSONReader.of(json));
        }catch (Exception e){}
        String s = null;

        if (obj == null) {
            s = null;
        }else{
            if (obj instanceof Integer) {
                s = "Integer";
            }else if (obj instanceof Long) {
                s = "Long";
            }else if (obj instanceof BigDecimal) {
                s = "BigDecimal";
            }else if (obj instanceof String) {
                s = "String";
            }else{
                s = "Unknown";
            }
        }

        return s;
    }
}
