package org.thomas.hive.udf;

import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * 作用: 获取json中某个key对应的value的数据类型(用来识别json value类型是否符合预期). 只支持区分long和string
 * 解决痛点: hive自带的get_json_object无法识别数据类型,都按string处理
 *      create table t1 as select get_json_object('{"age":"22"}','$.age') as c1;  -- c1 String
 *      create table t3 as select get_json_object('{"age":22}','$.age') as c3;  -- c3 Long
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
            s = obj instanceof String ? "String" : "Long";
        }

        return s;
    }
}
