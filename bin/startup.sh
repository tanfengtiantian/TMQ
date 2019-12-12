#!/bin/bash

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
export LANG=en_US.UTF-8
export BASE=$base
# 进程文件是否存在
if [ -f $base/bin/tfkafka.pid ] ; then
	echo "found tfkafka.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

# 创建日志目录
if [ ! -d $base/logs ] ; then
	mkdir -p $base/logs
fi

# set java path /usr/local/java/jdk1.8.0_121/bin/java
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

# 64位 设置2Gxms
str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
	JAVA_OPTS="-server -Xms2048m -Xmx3072m -Xmn1024m -XX:SurvivorRatio=2 -XX:PermSize=96m -XX:MaxPermSize=256m -Xss256k -XX:-UseAdaptiveSizePolicy -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError"
else
	JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m "
fi
# 追加jvm参数
JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
# 应用名称
ADAPTER_OPTS="-DappName=tf-kafka -DlogHome=$base"

# set CLASSPATH 
for i in $base/lib/*;
    do CLASSPATH=$i:"$CLASSPATH";
done

CLASSPATH="$base/conf:$CLASSPATH";

echo "cd to $bin_abs_path for workaround relative path"
cd $bin_abs_path

echo CLASSPATH :$CLASSPATH
# 0 表示stdin标准输入 ,1 表示stdout标准输出 ,2 表示stderr标准错误 2>&1 合并输出log
$JAVA $JAVA_OPTS $ADAPTER_OPTS -classpath .:$CLASSPATH io.kafka.Bootstrap -FjettyBroker=jettyBroker.properties 1>>$base/logs/tfkafka.log 2>&1 &
echo $! > $base/bin/tfkafka.pid

echo "cd to $current_path for continue"
cd $current_path

