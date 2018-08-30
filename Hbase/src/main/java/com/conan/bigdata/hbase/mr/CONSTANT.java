package com.conan.bigdata.hbase.mr;

/**
 * Created by admin on 2017/1/5.
 */
public class CONSTANT {

    public static final String JOB_NAME = "USER_ACTION_TO_HBASE";

    public static final String TABLE_NAME = "user_action";
    public static final String FAMILY_NAME = "info";

    public static final String IN_PATH="/user/hive/warehouse/dmd.db/user_tag_detail/statis_month=2018-08/action_id=5";
    public static final String OUTPUT_PATH="/user/hadoop/hbase/user_action";

    public static final String EXT_LIBS="/user/hadoop/libs";

}
