package org.thomas.hive;

import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest{
    public static void main(String[] args) {
        Object extract = JSONPath.of("$[*].id").extract(JSONReader.of("[{\"id\":1},{\"id\":2}]"));
        System.out.println(extract);

    }
}
