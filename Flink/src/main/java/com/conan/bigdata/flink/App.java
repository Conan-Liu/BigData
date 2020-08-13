package com.conan.bigdata.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Logger LOG= LoggerFactory.getLogger("App");

    public static void main( String[] args )
    {
        LOG.info("info ...");
        LOG.debug("debug ...");
        LOG.warn("warn ...");
        LOG.error("error ...");
        System.out.println( "Hello World!" );
    }
}
