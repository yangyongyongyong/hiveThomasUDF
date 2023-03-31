package org.thomas.hive.udtf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;


/**
 *  hive udtf 函数
 *  用来展开jsonArray的字符串列
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
 *  create temporary function explode_json_array as "org.thomas.hive.udtf.ExplodeJsonArray";
 *  select * from tb1 lateral view explode_json_array(info) tmp_table_name AS ainfo;
 *
 *  输出:
 *  | name | info | ainfo |
 * | :--- | :--- | :--- |
 * | thomas | \[{"k1":"v1"},{"k2":"v2"}\] | {"k1":"v1"} |
 * | thomas | \[{"k1":"v1"},{"k2":"v2"}\] | {"k2":"v2"} |
 * | peter | \[{"k4":"v4"},{"k66":"v66"}\] | {"k4":"v4"} |
 * | peter | \[{"k4":"v4"},{"k66":"v66"}\] | {"k66":"v66"} |
 */
public class ExplodeJsonArray extends GenericUDTF { //ctrl + h 去他的其他实现类抄代码

    // fieldOIs fieldNames 必须在外面定义 在initialize方法中初始化 否则报错:java.lang.ClassCastException: org.apache.hadoop.hive.serde2.lazy.LazyString cannot be cast to java.lang.String
    private List<ObjectInspector> fieldOIs;
    private List<String> fieldNames;

    private ArrayList<String> datas = new ArrayList<String>();

    // 用来指定输出数据的schema
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 定义udtf输出的多列的列名
        fieldNames = new ArrayList();
        fieldNames.add("udtf_out_col1"); //这个没啥用 会被你给的别名覆盖.

        // 定义udtf输出的多列的类型
        fieldOIs = new ArrayList();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //对应hive string

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // udtf输入的参数 通过forward(...)一个个发出去铺平(为多行)
    public void process(Object[] objects) throws HiveException {
        if (StringUtils.isNotBlank(objects[0].toString())) {
            //有数据 且 非空
            JSONArray jsarr = JSON.parseArray(objects[0].toString());
            for (int i = 0; i < jsarr.size(); i++) {
                datas.clear();
                datas.add(jsarr.getString(i));
                datas.add("test");
                forward(datas);
            }
        }
    }

    // 可以不写内容
    public void close() throws HiveException {

    }
}
