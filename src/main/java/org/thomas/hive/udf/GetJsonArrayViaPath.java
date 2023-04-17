package org.thomas.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * desc:
 *      - 返回 hive array[String]
 */
public class GetJsonArrayViaPath extends UDF {
    public ArrayList<String> evaluate(String json, String path) {
        if (json == null || path == null) {
            return null;
        }
        ArrayList<String> arr = new ArrayList<String >();

        try {
            JSONArray js_arr = JSON.parseArray(JSONPath.of(path).extract(JSONReader.of(json)).toString());
            // js_arr 转换为java数组
            for (int i = 0; i < js_arr.size(); i++) {
                arr.add(js_arr.getString(i));
            }
        }catch (Exception e){}

        return arr;
    }
}
