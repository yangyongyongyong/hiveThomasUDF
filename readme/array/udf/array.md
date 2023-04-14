
```hiveql
create temporary function castArrayType2Int as 'org.thomas.hive.udf.ArrayCastType.CastArrayT1Cast2Int';
-- select castArrayType2Int(array('1','2')) ;
    -- [1,2]

create temporary function castArrayType2Double as 'org.thomas.hive.udf.ArrayCastType.CastArrayT1Cast2Double';
-- select castArrayType2Double(array('1.11','2.22')) ;
-- [1.11,2.23]

create temporary function castArrayType2Long as 'org.thomas.hive.udf.ArrayCastType.CastArrayT1Cast2Long';
-- select castArrayType2Long(array(123456789012,1234567890123)) ;
-- [123456789012,1234567890123]

create temporary function array_union_uniq as "org.thomas.hive.udf.ArrayUnionUDF";
-- select array_union_uniq(`array`(1,2),`array`(2,3)) ;
    -- [1,2,3] Array<Int> 类型自适应
-- select array_union_uniq(`array`('0','111'),`array`('2'),`array`('33')) ;
    -- ["0","111","2","33"] Array<String> 类型自适应
-- 合并数据 不去重 可以用 select split(concat_ws(',',array("john", "james"), array('peter'), array("sam","peter")), ","); 或者 array_union_all

create temporary function array_union_all as "org.thomas.hive.udf.ArrayUnionALLUDF";
-- select array_union_all(`array`(1,2,3),`array`(2,3,4));
    -- [1,2,3,2,3,4]  合并数组 不去重

create temporary function array_flatten as "org.thomas.hive.udf.ArrayFlattenUDF";
-- select array_flatten(`array`(`array`(1,2),`array`(2,3))); 类型自适应
    -- [1,2,2,3]

create temporary function array_intersect as "org.thomas.hive.udf.ArrayIntersectUDF";
-- select array_intersect(`array`(5,1,2,8),`array`(2,3,5,7),`array`(5,8)); -- array 交集
    -- [5]

create temporary function array_position as "org.thomas.hive.udf.ArrayIndexOfUDF";
-- select array_position(`array`(11,22,33,44),33); -- 返回指定元素在数组列中的index. index从0开始. 不存在则返回 -1
    -- 3

create temporary function array_remove as 'org.thomas.hive.udf.ArrayRemove';
-- select array_remove(`array`(11,22,33,22,100),22,100);
    -- [11,33]

create temporary function array_except as 'org.thomas.hive.udf.ArrayExcept';
-- select array_except(`array`(11,22,33,22,100),`array`(22),`array`(100));
    -- [11,33]
```
