if [ -z "$DOCKER_HOST_IP" ] ; then
    if [ -z "$DOCKER_HOST" ] ; then
      export DOCKER_HOST_IP=`hostname`
    else
      echo using ${DOCKER_HOST?}
      XX=${DOCKER_HOST%\:*}
      export DOCKER_HOST_IP=${XX#tcp\:\/\/}
    fi
fi

echo DOCKER_HOST_IP is $DOCKER_HOST_IP

export EVENTUATE_EVENT_TRACKER_ITERATIONS=120
export EVENTUATE_REDIS_SERVERS=${DOCKER_HOST_IP}:6379,${DOCKER_HOST_IP}:6380,${DOCKER_HOST_IP}:6381
export EVENTUATE_REDIS_PARTITIONS=2