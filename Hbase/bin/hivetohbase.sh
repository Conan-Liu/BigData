#!/bin/bash
#数据库
db=dmd
#需要导数据的表
hive_table=user_tag_detail
hive_table_new=user_tag_detail_new
#主键
rowkey=row_key

#刷新keytab权限
#kinit -kt /home/deploy/huangwt/utag.keytab utag@MWEER.COM
/usr/bin/kinit -kt /opt/etl_works/keytab_file/hive.keytab hive@MWEE.CN

echo "##################################[step 1 generate splites]#####################################"
hdfs dfs -rm -r /user/test/user_tag/hbase_splits_file

/etc/alternatives/beeline -u 'jdbc:hive2://dn1.hadoop.pdbd.mwbyd.cn:10000/;principal=hive/dn1.hadoop.pdbd.mwbyd.cn@MWEE.CN' -e "
    set spark.network.timeout=3000;
    use ${db};

    CREATE EXTERNAL TABLE IF NOT EXISTS hbase_splits6(partition STRING, count int)
    PARTITIONED BY (table STRING);

    create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

    INSERT OVERWRITE TABLE hbase_splits6
    PARTITION (table='${hive_table}')
    select ${rowkey},row_sequence() from (
        select
            ${rowkey},
            row_sequence() as row
        from (
            select
                ${rowkey}
            from ${hive_table} tablesample(bucket 1 out of 100 on ${rowkey}) s order by ${rowkey}
        ) t order by ${rowkey}
    ) x where (row % 23000)=0 order by ${rowkey} ;

    drop table hbase_splits_file;

    CREATE EXTERNAL TABLE IF NOT EXISTS hbase_splits_file(partition STRING)
    PARTITIONED BY (table STRING)
    ROW FORMAT
      SERDE 'org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe'
    STORED AS
      INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat'
    LOCATION '/user/test/user_tag/hbase_splits_file';

    INSERT OVERWRITE TABLE hbase_splits_file
    PARTITION (table='${hive_table}')
    select partition from hbase_splits6 where table='${hive_table}';
"

echo "##################################[step 2 create hfile table ]#####################################"

/etc/alternatives/beeline -u 'jdbc:hive2://dn1.hadoop.pdbd.mwbyd.cn:10000/;principal=hive/dn1.hadoop.pdbd.mwbyd.cn@MWEE.CN' -e "use ${db};
set spark.network.timeout=3000;
drop table hbase_${hive_table};

create table hbase_${hive_table}(
row_key string,
id string,
mw_id string,
card_no string,
action string,
state_id string,
state_name string,
business_time string,
brand_id string,
brand_name string,
shop_id string,
shop_name string,
category_name string,
city_id string,
city_name string,
province_id string,
province_name string,
bc_name string,
action_attr string,
action_id string)
stored as
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.hbase.HiveHFileOutputFormat'
TBLPROPERTIES ('hfile.family.path' = '/user/test/user_tag/user_tag_detail_sort/cf');
"
echo "##################################[step 3 create hfile ]#####################################"
task_num=$(
    /etc/alternatives/beeline -u 'jdbc:hive2://dn1.hadoop.pdbd.mwbyd.cn:10000/;principal=hive/dn1.hadoop.pdbd.mwbyd.cn@MWEE.CN' -e "
        set spark.network.timeout=3000;
        use ${db};
        select max(count) + 1 from hbase_splits6 where table='${hive_table}';
    "
)


task_num_str=$(echo ${task_num})


num=$(echo "${task_num_str}" | awk '{print $7}')
echo ${num}


/etc/alternatives/beeline -u 'jdbc:hive2://dn1.hadoop.pdbd.mwbyd.cn:10000/;principal=hive/dn1.hadoop.pdbd.mwbyd.cn@MWEE.CN' -e "
set spark.network.timeout=3000;
use ${db};
SET mapred.reduce.tasks=${num};
SET total.order.partitioner.path=/user/test/user_tag/hbase_splits_file/table=${hive_table}/000000_0;
SET hive.mapred.partitioner=org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
set hive.optimize.sampling.orderby=true;
set hive.optimize.sampling.orderby.number=10000000;
set hive.optimize.sampling.orderby.percent=0.1f;

INSERT OVERWRITE TABLE hbase_${hive_table}
SELECT row_key ,
id ,
mw_id ,
card_no,
action ,
state_id ,
state_name ,
business_time ,
brand_id ,
brand_name ,
shop_id ,
shop_name ,
category_name ,
city_id ,
city_name ,
province_id ,
province_name ,
bc_name ,
action_attr ,
action_id FROM ${hive_table} CLUSTER BY ${rowkey};
"

status=$?
echo status=${status}
if [ ${status} -eq 0 ];
then
    echo "##################################[step 4 create hbase table ]#####################################"

    echo "create '${hive_table_new}',{NAME => 'cf', COMPRESSION => 'GZ'}" | hbase shell

    for f in /opt/cloudera/parcels/CDH/jars/*; do export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f; done

    echo "##################################[step 5 move hfile to hbase ]#####################################"
    export HADOOP_CONF_DIR=/etc/hbase/conf:/etc/hadoop/conf:/etc/hive/conf
    hadoop jar  /opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/jars/hbase-server-1.2.0-cdh5.15.0.jar completebulkload -Dhbase.zookeeper.quorum=10.1.39.99 -Dhbase.zookeeper.property.clientPort=2181  -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024  /user/test/user_tag/user_tag_detail_sort  ${hive_table_new}

    echo "disable  '${hive_table}'" |hbase shell
    echo "drop  '${hive_table}'" |hbase shell
    echo "disable '${hive_table_new}'" |hbase shell
    echo "snapshot  '${hive_table_new}','${hive_table_new}_snapshot'" |hbase shell
    echo "clone_snapshot  '${hive_table_new}_snapshot','${hive_table}'" |hbase shell
    echo "delete_snapshot  '${hive_table_new}_snapshot'" |hbase shell
    echo "drop  '${hive_table_new}'" |hbase shell

    echo "##################################[step 6 test ]#####################################"
    echo "scan '${hive_table}', { LIMIT => 1 }" | hbase shell
else
    echo "ERROR:@@@@@@ generate hfile error @@@@@@";
    exit -1;
fi