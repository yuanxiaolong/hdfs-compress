package com.yxl.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * 属性文件工具类
 *
 * Created by yuanxiaolong on 15/6/26.
 */
public class PropertyUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyUtil.class);

    private static String propFile = "/global.properties";

    private PropertyUtil() {

    }

    private static class ProoertyHolder {

        private static Properties prop=new Properties();
        static{
            try {
                InputStream in = ProoertyHolder.class.getResourceAsStream(propFile);
                prop.load(in);
            }  catch (Exception e) {
                LOG.error("[ERROR]Load global.properties failed", e);
            }
        }
    }

    public static Properties getInstance() {
        return ProoertyHolder.prop;
    }

}
