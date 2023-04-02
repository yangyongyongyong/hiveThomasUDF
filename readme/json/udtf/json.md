
```hiveql
create temporary function explode_json_array as "org.thomas.hive.udtf.ExplodeJsonArray";
-- select name,ainfo from tb1 lateral view explode_json_array('[{"k1":"v1"},{"k2":"v2"}]') tmp_table_name AS ainfo limit 2;
    -- +------+-----------+
    -- |name  |ainfo      |
    -- +------+-----------+
    -- |thomas|{"k1":"v1"}|
    -- |thomas|{"k2":"v2"}|
    -- +------+-----------+
```