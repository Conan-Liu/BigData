package com.conan.bigdata.common.javaapi;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

public class LogExp {

    // slf4j的日志标准，使用log4j来收集日志
    private static final Logger LOG= LoggerFactory.getLogger(LogExp.class);

    public static void main(String[] args) {

        LOG.debug("普通日志记录");

        // 占位符日志记录
        // 日志可以配置输出级别，如果是普通的字符串连接，那么会先执行连接，然后判断是否输出，如果级别不够输出，这种操作显然浪费资源
        // 而占位符可以实现不用输出的日志，不执行字符串连接
        for (int i=0;i<3;i++){
            LOG.info("这是第 {} 条记录",i);
        }
        // 当然也可以通过if来判断并输出
        if(LOG.isDebugEnabled()){
            LOG.debug("这是debug");
        }

        // 注意: {}的作用是调用对应参数的toString()方法格式化日志，如果exception放在对应参数的位置上也会被格式化
        // 所以，e要放在{}参数之外,而且只能放在最后一个。如果放在中间也不会被打印错误信息
        try {
            throw new FileNotFoundException("未找到文件");
        }catch (Exception e){
            LOG.error("error:can't found file [{}]","/path",e);
        }


        // 用 \ 转义
        LOG.debug("set \\{} differs from {}","3");

        // 两个参数，以此类推
        LOG.debug("两个占位符，可以传两个参数{} ---- {}",1,2);


    }
}
