#!/bin/sh
. /etc/profile
. ~/.bashrc
#set -x
basep="/var/lib/hadoop-hdfs/getdata"
#basep="/home/hadoop/workspace/getdata"
when="-1"
today=`date  +"%Y-%m-%d" -d  "$when days"`
indexday=`date  +"%Y.%m.%d" -d  "$when days"`
#host="elk.mwbyd.cn"
host="10.1.26.153:9200"
#host="elkdata.mwbyd.cn"
rm -fr $basep/log/*
cat $basep/a_tmp.txt|grep -v "#"|while read line
do
echo "###############################################"
business=`echo ${line}|awk '{print $1}'`
module=`echo ${line}|awk '{print $2}'`
action=`echo ${line}|awk '{print $3}'`
echo ${business}_${module}_${action}
$basep/esm -s http://$host -q "module:${module} AND action:${action}" -x ${business}-${indexday} -o ${basep}/log/${business}_${module}_${action}.log 2>&1
cat ${basep}/log/${business}_${module}_${action}.log |awk -F'source\":' '{print $2}'|awk -F",\"_type" '{print $1}' >${basep}/log/${indexday}.log
gzip ${basep}/log/${indexday}.log

hdfs dfs -rm /repository/kafka/${business}_${module}_${action}/$today/${indexday}.log.gz
#hdfs dfs -ls /repository/kafka/${business}_${module}_${action}/$today/${indexday}.log
#
hdfs dfs -mkdir -p /repository/kafka/${business}_${module}_${action}/$today/
#
#ls -lrth  $basep/log/${indexday}.log
echo "hdfs -put $basep/log/${indexday}.log.gz /repository/kafka/${business}_${module}_${action}/$today"
hdfs dfs -put $basep/log/${indexday}.log.gz /repository/kafka/${business}_${module}_${action}/$today/
rm -f $basep/log/${indexday}.log.gz
#ls -lrt $basep/log/
date
echo "###############################################"
done

#set -x
