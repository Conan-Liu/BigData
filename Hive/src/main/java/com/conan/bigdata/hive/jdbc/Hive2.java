package com.conan.bigdata.hive.jdbc;

import java.sql.*;

/**
 * Created by Administrator on 2018/8/1.
 */
public class Hive2 {

    public static Connection conn = null;

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://dn1.hadoop.pdbd.prod:10000/default", "hive", "hive");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("set mapreduce.job.name = user_tag_detail");
        System.out.println(ps.getClass().getName());
        ps.addBatch("set hive.exec.dynamic.partition.mode = nonstrict");
        ps.addBatch("set mapreduce.job.name = user_tag_detail");
        ps.addBatch("insert overwrite table dmd.user_tag_detail_new partition (action_id)\n" +
                "select\n" +
                "    /*+ mapjoin(s1) */\n" +
                "    concat(a.mw_id, unix_timestamp(a.business_time, 'yyyy-MM-dd HH:mm:ss'), a.action_id, ceil(rand()*100000)) as row_key,\n" +
                "    a.id,\n" +
                "    a.mw_id,\n" +
                "    a.action,\n" +
                "    a.state_id,\n" +
                "    a.state_name,\n" +
                "    a.business_time,\n" +
                "    nvl(s2.shopid, '') as brand_id,\n" +
                "    nvl(s2.shopname, '') as brand_name,\n" +
                "    nvl(s1.shopid, '') as shopid,\n" +
                "    nvl(s1.shopname, '') as shop_name,\n" +
                "    nvl(s1.categoryname, '') as category_name,\n" +
                "    nvl(s1.cityid, '') as cityid,\n" +
                "    nvl(s1.cityname, '') as cityname,\n" +
                "    nvl(s1.provinceid, '') as provinceid,\n" +
                "    nvl(s1.provincename, '') as provincename,\n" +
                "    nvl(s1.bcname, '') as bcname,\n" +
                "    a.action_attr,\n" +
                "    a.action_id\n" +
                "from\n" +
                "(\n" +
                "    select\n" +
                "        a.id,\n" +
                "        coalesce(b.mw_id, d.mw_id, c.mw_id, '') as mw_id,\n" +
                "        a.action_id,\n" +
                "        a.action,\n" +
                "        a.state_id,\n" +
                "        a.state_name,\n" +
                "        a.business_time,\n" +
                "        a.brand_id,\n" +
                "        a.shop_id,\n" +
                "        a.action_attr\n" +
                "    from dmd.user_tag_detail_new_test a\n" +
                "    left join dmd.user_info_temp b\n" +
                "        on (case when a.user_id = '' then concat('', rand()) else a.user_id end) = b.accountname and b.flag = '1'-- and b.account_type = 'userid'\n" +
                "    left join dmd.user_info_temp c\n" +
                "        on (case when a.mobile = '' then concat('', rand()) else a.mobile end) = c.accountname and c.flag = '2'\n" +
                "    left join dmd.user_info_temp d\n" +
                "        on (case when a.open_id = '' then concat('', rand()) else a.open_id end) = d.accountname and d.flag = '3'\n" +
                "    where coalesce(b.mw_id, d.mw_id, c.mw_id, '') <> ''\n" +
                ") a\n" +
                "left join dmd.shoptable_test s1\n" +
                "    on (case when a.shop_id = 0 then concat('', rand()) else a.shop_id end) = s1.shopid\n" +
                "left join dmd.shoptable_test s2\n" +
                "    on a.brand_id = s2.shopid\n" +
                "where s1.shopid is not null or s2.shopid is not null");
        ps.executeBatch();
        conn.commit();

    }
}