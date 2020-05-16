#! /bin/sh
name="segi-license-tool-1.0"
Xmx=512m
Xms=512m
Xmn=256m
pid=${name}".pid"
logName="all"
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/config

if [ ! -d "${DEPLOY_DIR}/logs/" ];then
mkdir ${DEPLOY_DIR}/logs
else
echo "日志输出到logs文件夹下"
fi

jarName=`find ./boot/ -name ${name}.jar | sort -r | head -n 1`
if [ -f "$pid" ]
  then
    echo "$jarName is running !"
    exit 0;
  else
    echo -n  "start ${jarName} ..."
    nohup java   -Xmx${Xmx} -Xms${Xms} -Xmn${Xmn} -XX:CMSFullGCsBeforeCompaction=3 -XX:CMSInitiatingOccupancyFraction=60 -XX:-OmitStackTraceInFastThrow -jar ${jarName} --CONF_DIR=${CONF_DIR}  --spring.config.location=${CONF_DIR}/  --logging.config=${CONF_DIR}/logback-spring.xml >${DEPLOY_DIR}/logs/all.log 2>&1 &
    [ $? -eq 0 ] && echo   "[ok]"
    echo $! > ${DEPLOY_DIR}/bin/${pid}   # 将jar包启动对应的pid写入文件中，为停止时提供pi
 fi
