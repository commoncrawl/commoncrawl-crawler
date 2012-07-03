#!/usr/bin/env bash


# Environment Variables
#
#   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
#
#   HADOOP_HOME      Location of the HADOOP INSTALL
#
#   CCAPP_HEAPSIZE   APP HEAP SIZE 
#
#   CCAPP_CLASSPATH  Extra Java CLASSPATH entries.
#
#   CCAPP_HEAPSIZE   The maximum amount of heap to use, in MB. 
#                    Default is 1000.
#
#   CCAPP_OPTS      Extra Java runtime options.
#
#   CCAPP_CONF_DIR  Alternate conf dir. Default is ${CCAPP_HOME}/conf.
#
#   CCAPP_ROOT_LOGGER The root appender. Default is INFO,console
#

rotate_run_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}


bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# if no args specified, show usage
if [ $# = 0 ]; then
  echo "Usage: ccAppRun [options] (start/stop) appname [parameters]"
  echo "where appname is fully qualified class name:"
  
  exit 1
fi

#echo "----------------------------------------------------------------------------------------"
#echo "CONFIG"
#echo "----------------------------------------------------------------------------------------"

. "$bin"/ccApp-config.sh

if [ "$HADOOP_HOME" = "" ]; then 
  echo "Error: HADOOP_HOME is not set."
  exit 1
fi

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi


# extract jar 
if [ "${CCAPP_JAR_PATH}/${CCAPP_JAR}" -nt "${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}/${CCAPP_JAR}" ]; then
  # remove contents if neccessary ... 
  if [ -d "${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}" ]; then 
    rm -r -f "${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}/*"
  else
    mkdir "${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}"
  fi
  ORIG_LOC=`pwd`
  #echo "entering ${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}"
  cd "${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}"
  #echo "copying jar to runlib_${CCAPP_RUNLIB_PREFIX}"
  cp ${CCAPP_JAR_PATH}/${CCAPP_JAR} ${CCAPP_JAR}
  #echo "extracting ${CCAPP_JAR_PATH}/${CCAPP_JAR}"
  # now extract the jar ... 
  jar -xf ${CCAPP_JAR_PATH}/${CCAPP_JAR}
  cd $ORIG_LOC
fi

#figure out jar timestamp ... 
CCAPP_BUILD_DATE=$(ls -l ${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}/${CCAPP_JAR} | awk '{ print $6"-"$7"-"$8 }')

#echo "Jar Build Time is:"$CCAPP_BUILD_DATE

# CLASSPATH initially contains CCAPP_CONF:HADOOP_CONF_DIR
CLASSPATH=${CCAPP_CONF_DIR}
CLASSPATH=${CLASSPATH}:${HADOOP_CONF_DIR}
# and add in test path ... 
CLASSPATH=${CLASSPATH}:${CCAPP_HOME}/tests
# next add tools.jar
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
# next add runlib
CLASSPATH=${CLASSPATH}:${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}
# next add commoncrawl jar FIRST ... 
# CLASSPATH=${CLASSPATH}:${CCAPP_JAR_PATH}/${CCAPP_JAR}
# reference from runlib directory so building a new jar won't impact server until deployment to runlib
if [ ${CCAPP_JAR_IN_CLASSPATH} != 1 ]; then
	CLASSPATH=${CLASSPATH}:${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}/${CCAPP_JAR}
fi
# then add nested libraries in commoncrawl jar (via runlib)
for f in ${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

#next add hadoop jar path 
CLASSPATH=${CLASSPATH}:${HADOOP_JAR}
#  add the sqlite jar 
CLASSPATH=${CLASSPATH}:${CCAPP_SQLITE_JARLOCATION}
# add hadoop libs to CLASSPATH
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
# and add jetty libs ... 
for f in $HADOOP_HOME/lib/jetty-ext/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done


# setup 'java.library.path' for native-hadoop code if necessary


#establish java platform name 
JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} org.apache.hadoop.util.PlatformName | sed -e 's/ /_/g' | sed -e "s/ /_/g"`
#setup commoncrawl library paths
JAVA_LIBRARY_PATH="${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}":"${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}"/lib/native/${JAVA_PLATFORM}:/usr/local/lib
#add in mozilla parser stuff 
JAVA_LIBRARY_PATH="$JAVA_LIBRARY_PATH:${CCAPP_HOME}/lib/MozillaParser-v-0-3-0/dist/${JAVA_PLATFORM}"
#next hadoop 
if [ -d "${HADOOP_HOME}/build/native" -o -d "${HADOOP_HOME}/lib/native" ]; then
  
  if [ -d "$HADOOP_HOME/build/native" ]; then
    JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/build/native/${JAVA_PLATFORM}/lib
  fi
  
  if [ -d "${HADOOP_HOME}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi
# now add in sqlite3 java library path ... 
JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:${CCAPP_SQLITE_LIBRARYPATH}"
echo "JAVA_LIBRARY_PATH:$JAVA_LIBRARY_PATH"

# figure out which class to run
if [ "$CCAPP_NAME" = "db" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.database.CrawlDBServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "master" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.master.CrawlMasterServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "crawler" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.crawler.CrawlerServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "qmaster" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.queryserver.master.MasterServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "qslave" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.queryserver.slave.SlaveServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "prslave" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.pagerank.slave.PageRankSlaveServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "prmaster" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.pagerank.master.PageRankMaster'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "dservice" ] ; then
  CCAPP_CLASS='org.commoncrawl.directoryservice.DirectoryServiceServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "dnsservice" ] ; then
  CCAPP_CLASS='org.commoncrawl.dnsservice.DNSServiceServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "crawlerproxy" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.crawler.tests.TestHarness'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "proxyserviceProd" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.proxy.ProxyServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "proxyserviceDbg" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.proxy.ProxyServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "crawlhistory" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.crawler.history.CrawlHistoryServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "statsservice" ] ; then
  CCAPP_CLASS='org.commoncrawl.statsservice.StatsServiceServer'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
elif [ "$CCAPP_NAME" = "crawlstats" ] ; then
  CCAPP_CLASS='org.commoncrawl.crawl.statscollector.CrawlStatsCollectorService'
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=1
else
  CCAPP_CLASS=$CCAPP_NAME
  CCAPP_VMARGS="-Dcom.sun.management.jmxremote $CCAPP_VMARGS" 
  CCAPP_IS_CC_SERVER=0
  CCAPP_ROOT_LOGGER="INFO,console,DRFA"
fi

if [ -e /dev/jdk_urandom_hack ]; then
	echo "**************************************"
	echo "WARNING: USING /dev/jdk_urandom_hack as seed random source!!!"
	echo "**************************************"
	CCAPP_VMARGS="-Djava.security.egd=file:/dev/jdk_urandom_hack $CCAPP_VMARGS" 
fi
# jvm bug work around 
CCAPP_VMARGS="-XX:CompileCommand=exclude,org/commoncrawl/io/internal/NIOSocketSelector,poll $CCAPP_VMARGS"

#echo "CCAPP_CLASS:$CCAPP_CLASS"

CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.classes.root=${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}/"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.build.date=$CCAPP_BUILD_DATE"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.log.dir=$CCAPP_LOG_DIR"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.log.file=$CCAPP_LOG_FILE".log
CCAPP_VMARGS="$CCAPP_VMARGS -Dhadoop.home.dir=$HADOOP_HOME"
#HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.id.str=$HADOOP_IDENT_STRING"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.root.logger=${CCAPP_ROOT_LOGGER:-INFO,DRFA}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  CCAPP_VMARGS="$CCAPP_VMARGS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi  
	
log=$CCAPP_LOG_DIR/${CCAPP_LOG_FILE}_run.log
pid=$CCAPP_PID_DIR/${CCAPP_NAME}_app_host_$CCAPP_HOSTNAME.pid


#echo $log
#echo $pid

#and ld_library path 
export LD_LIBRARY_PATH="${CCAPP_HOME}/runlib_${CCAPP_RUNLIB_PREFIX}"/lib/native/${JAVA_PLATFORM}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}:$LD_LIBRARY_PATH

#mozilla library path 

#if mozilla installed in /usr/local/lib, use that path ... 
if [ -e /usr/local/lib/parserWorker ]; then

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib/mozilla
export MOZILLA_LIB_PATH="/usr/local/lib"
chmod '+x' $MOZILLA_LIB_PATH/parserWorker

else 

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CCAPP_HOME}/lib/MozillaParser-v-0-3-0/dist/${JAVA_PLATFORM}:${CCAPP_HOME}/lib/MozillaParser-v-0-3-0/dist/${JAVA_PLATFORM}/mozilla
#mozilla lib path
export MOZILLA_LIB_PATH="${CCAPP_HOME}/lib/MozillaParser-v-0-3-0/dist/${JAVA_PLATFORM}"
chmod '+x' $MOZILLA_LIB_PATH/parserWorker

fi

echo "**MOZILLA_LIB_PATH:"${MOZILLA_LIB_PATH}
echo "**LD_LIBRARY_PATH:"${LD_LIBRARY_PATH}

export CLASSPATH=$CLASSPATH

case $CCAPP_ACTION in

    (start)
    
    mkdir -p "$CCAPP_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
    
    # rotate run log ... 
    rotate_run_log $log
    echo starting $CCAPP_NAME, logging to $log


		if [ $CCAPP_IS_CC_SERVER = 1 ]; then
			#echo "runing commoncrawl server"
				# run command
				CCAPP_CMD_LINE="$JAVA $JAVA_HEAP_MAX $CCAPP_VMARGS org.commoncrawl.server.CommonCrawlServer --server $CCAPP_CLASS $CCAPP_ARGS $CCAPP_ARGS2"  
		else
				#echo "runing java class"
				CCAPP_CMD_LINE="$JAVA $JAVA_HEAP_MAX $CCAPP_VMARGS $CCAPP_CLASS $CCAPP_ARGS $CCAPP_ARGS2"  
		fi
    
    #echo "----------------------------------------------------------------------------------------"
    #echo "CMD LINE"
    echo "----------------------------------------------------------------------------------------"
    echo $CCAPP_CMD_LINE
    echo "----------------------------------------------------------------------------------------"
		
		if [ "$CCAPP_NOHUP" == 1 ]; then
				nohup $CCAPP_CMD_LINE  > "$log" 2>&1 < /dev/null &
				echo $! > $pid
				echo "Process PID:"$!
				sleep 1; head "$log"
				exit 0    
		elif [ "$CCAPP_GDB" == 1 ]; then
				echo "running via gdb"
				gdb --args $CCAPP_CMD_LINE
				exit 0    
		else
				$CCAPP_CMD_LINE
				exit 0
		fi
		
    ;;
    
    (stop)
    
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $CCAPP_NAME
        kill `cat $pid`
      else
        echo $CCAPP_NAME is not running 
      fi
    else
      echo $CCAPP_NAME is not running
    fi

    ;;
    
    (*)
    
    echo Invalid Action. Supported actions are start or stop.
    exit 1
    
    ;;
    
esac

