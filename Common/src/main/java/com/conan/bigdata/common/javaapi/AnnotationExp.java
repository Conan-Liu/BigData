package com.conan.bigdata.common.javaapi;

/**
 * 自定义注解
 */
public @interface AnnotationExp {

    /**
     * 内部定义的方法，作为注解的参数信息
     * 注解和接口差不多，所以方法不用写修饰符，默认public，
     *
     * 方法名和注解的参数是一一对应的，注解参数必须全部指定，除非使用default指定默认值
     */

    /**
     * 注解的时候，一定需要指定name参数，否则报错，使用default指定默认值后，对应参数可以省略
     */
    String name() default "";

    // 数组指定默认值
    int[] age() default {};
}