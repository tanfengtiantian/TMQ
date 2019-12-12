#!/bin/bash

base=/usr/local/javakafka-1.0.0
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

JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m "

# 追加jvm参数
JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
# 应用名称 loghome
ADAPTER_OPTS="-DappName=tf-kafka -DlogHome=$base"

# set CLASSPATH 
for i in $base/lib/*;
    do CLASSPATH=$i:"$CLASSPATH";
done

CLASSPATH="$base/conf:$CLASSPATH";

echo "cd to $bin_abs_path for workaround relative path"
cd $bin_abs_path

echo CLASSPATH :$CLASSPATH
# 0 表示stdin标准输入 ,1 表示stdout标准输出 ,2 表示stderr标准错误 docker不允许后台启动
java $JAVA_OPTS $ADAPTER_OPTS -classpath .:$CLASSPATH io.kafka.Bootstrap
echo $! > $base/bin/tfkafka.pid

echo ".....ok"


