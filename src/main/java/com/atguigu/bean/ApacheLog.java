package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 13:59
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String ip;
    private long eventTime;
    private String method;
    private String url;
}
