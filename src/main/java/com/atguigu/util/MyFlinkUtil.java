package com.atguigu.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/14 9:24
 */
public class MyFlinkUtil {
    public static <T> List<T> iterable2List(Iterable<T> it) {
        ArrayList<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }
        return list;
    }
    
    
    
}
