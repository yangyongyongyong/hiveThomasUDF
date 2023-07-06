
```hiveql
create temporary function size_json_array as "org.thomas.hive.udf.JsonArraySize";
-- select size_json_array('[{"k1":"v1"},{"k2":"v2"}]') ;
    -- 2

create temporary function get_json_object_via_path as 'org.thomas.hive.udf.GetJsonObjectViaPath';
-- select get_json_object_via_path('[{"k1":11,"k2":22},{"k1":44,"k2":55}]','$[*].k1') ;
    -- [11,44] 这里不是数组 是string 
-- select get_json_object_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des[0]');
    -- i am 0

create temporary function get_json_object_type_via_path as 'org.thomas.hive.udf.GetJsonObjectTypeViaPath';
-- select get_json_object_type_via_path('{"age":"22"}','$.age'); -- String
-- select get_json_object_type_via_path('{"age":22}','$.age'); -- Integer
-- select get_json_object_type_via_path('{"age":100000000000}','$.age'); -- Long
-- select get_json_object_type_via_path('{"age":1.11}','$.age'); -- BigDecimal
-- select get_json_object_type_via_path('{"age":"22"}','$.name'); -- null

create temporary function get_json_array_via_path as 'org.thomas.hive.udf.GetJsonArrayViaPath';
-- select get_json_array_via_path('{"k1":100,"k2":[11,22,33]}','$.k2') ;
    -- ["11","22","33"]  Array<String> 
-- select get_json_array_via_path('[{"k1":11,"k2":22},{"k1":44,"k2":55}]','$[*].k1') ;
    -- [11,44] 这里是数组
-- select get_json_array_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des');
    -- ["i am 0"]  -- jsonpath语法: https://alibaba.github.io/fastjson2/jsonpath_cn
-- select get_json_array_via_path('[{"des":"i am 1","id":1},{"des":"i am 2","id":2},{"des":"i am 0","id":0}]','$[?(@.id = 0)].des')[0];
    -- i am 0

create temporary function get_json_array as "org.thomas.hive.udf.GetJsonArray";
-- select name,field_name2 from tb1 lateral view explode(get_json_array('[{"k1":"v1"},{"k2":"v2"}]')) tmp_table_name AS field_name2 limit 2;
    -- +------+-----------+
    -- |name  |field_name2|
    -- +------+-----------+
    -- |thomas|{"k1":"v1"}|
    -- |thomas|{"k2":"v2"}|
    -- +------+-----------+
    
-- struct 转 json
create temporary function to_json as 'org.thomas.hive.udf.Convert.AnyToJson';
-- INSERT INTO default.tt1 (id, name, dt) VALUES (10, 'j', '2019-01-01');
select to_json(named_struct("id",id,"name",name,"dt",dt)) from tt1;
-- {"id":7,"name":"g","dt":"2019-01-01"}
```
