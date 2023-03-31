package org.thomas.hive.udf;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * hive udf 函数
 * 用来展开jsonArray的字符串列
 *
 * create table tb1(
 *     name string,
 *     info string
 * )
 * stored as parquet ;
 * insert overwrite table tb1 values
 *                           ('thomas','[{"k1":"v1"},{"k2":"v2"}]')
 *                           ,('peter','[{"k4":"v4"},{"k66":"v66"}]');
 *
 *  add jar /Users/thomas990p/IdeaProjects/hiveThomasUDF/target/hiveThomasUDF-1.0-SNAPSHOT-jar-with-dependencies.jar;
 *  create temporary function get_json_array as "org.thomas.hive.udf.GetJsonArray";
 *  select * from tb1 lateral view explode(get_json_array(info)) tmp_table_name AS field_name2;
 *
 *  输出:
 *  | name | info | field\_name2 |
 * | :--- | :--- | :--- |
 * | thomas | \[{"k1":"v1"},{"k2":"v2"}\] | {"k1":"v1"} |
 * | thomas | \[{"k1":"v1"},{"k2":"v2"}\] | {"k2":"v2"} |
 * | peter | \[{"k4":"v4"},{"k66":"v66"}\] | {"k4":"v4"} |
 * | peter | \[{"k4":"v4"},{"k66":"v66"}\] | {"k66":"v66"} |
 */
public class GetJsonArray extends UDF {
    // hive udf 传入JsonArray格式的string 返回字符串数组
    public ArrayList<String> evaluate(String jsonArr) {
        if (jsonArr == null) {
            return null;
        }
        JSONArray jsarr = JSON.parseArray(jsonArr);
        ArrayList<String> arr_jsonarr = new ArrayList<String>();
        for (int i = 0; i < jsarr.size(); i++) {
            arr_jsonarr.add(jsarr.getString(i));
        }
        return arr_jsonarr;
    }

}
