
```hiveql
create temporary function size_json_array as "org.thomas.hive.udf.JsonArraySize";
-- select size_json_array('[{"k1":"v1"},{"k2":"v2"}]') ;
    -- 2

create temporary function get_json_object_via_path as 'org.thomas.hive.udf.GetJsonObjectViaPath';
-- select get_json_object_via_path('[{"k1":11,"k2":22},{"k1":44,"k2":55}]','$[*].k1') ;
    -- [11,44] 这里不是数组 是string 

create temporary function get_json_array_via_path as 'org.thomas.hive.udf.GetJsonArrayViaPath';
-- select get_json_array_via_path('{"k1":100,"k2":[11,22,33]}','$.k2') ;
    -- ["11","22","33"]  Array<String> 
-- select get_json_array_via_path('[{"k1":11,"k2":22},{"k1":44,"k2":55}]','$[*].k1') ;
    -- [11,44] 这里是数组

create temporary function get_json_array as "org.thomas.hive.udf.GetJsonArray";
-- select name,field_name2 from tb1 lateral view explode(get_json_array('[{"k1":"v1"},{"k2":"v2"}]')) tmp_table_name AS field_name2 limit 2;
    -- +------+-----------+
    -- |name  |field_name2|
    -- +------+-----------+
    -- |thomas|{"k1":"v1"}|
    -- |thomas|{"k2":"v2"}|
    -- +------+-----------+
```