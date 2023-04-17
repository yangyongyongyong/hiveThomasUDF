package org.thomas.hive.udf;

import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * desc:
 *      - 返回值是string
 *      - demo:
 *          json:  "[{\"id\":1},{\"id\":2}]"
 *          path:  "$[*].id"
 *          return: "[1,2]"    String 注意 这里不是数组,如需数组请使用 get_json_array_via_path
 */
public class GetJsonObjectViaPath extends UDF {
    public String evaluate(String json, String path) {
        if (json == null || path == null) {
            return null;
        }
        String str = null;
        try{
            JSONPath.of(path).extract(JSONReader.of(json)).toString();
        }catch (Exception e){}

        return str;
    }

}
