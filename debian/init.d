#!/bin/sh
### BEGIN INIT INFO
# Provides:          ciel
# Required-Start:    $network $local_fs $remote_fs
# Required-Stop:     $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: a execution engine for distributed computing
# Description:       CIEL is designed to achieve high scalability
#                    and reliability when run on a commodity cluster
#                    or cloud computing platform. CIEL supports the
#                    full range of MapReduce-style computations, as
#                    well as Turing-powerful support for iterative
#                    and dynamic programming.
### END INIT INFO

# Author: Anil Madhavapeddy <anil@recoil.org>

PATH=/sbin:/usr/sbin:/bin:/usr/bin
DESC=ciel
NAME=ciel
CMD=/usr/bin/ciel
PIDFILE=/var/run/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME

# Exit if the package is not installed
[ -x $DAEMON ] || exit 0

# Read configuration variable file if it is present
[ -r /etc/default/$NAME ] && . /etc/default/$NAME

# Load the VERBOSE setting and other rcS variables
. /lib/init/vars.sh

# Define LSB log_* functions.
# Depend on lsb-base (>= 3.0-6) to ensure that this file is present.
. /lib/lsb/init-functions

if [ "$MASTER_BLOCK_STORE" = "" ]; then
  MASTER_BLOCK_STORE="$BLOCK_STORE/$MASTER_PORT"
fi

if [ "$WORKER_BLOCK_STORE" = "" ]; then
  WORKER_BLOCK_STORE="$BLOCK_STORE/$WORKER_PORT"
fi

LOGDIR=/var/log/ciel
MASTER_PID="/var/run/ciel.master.$MASTER_PORT.pid"
WORKER_PID="/var/run/ciel.worker.$WORKER_PORT.pid"
MASTER_CMD="master --logfile $LOGDIR/master.log -D --port $MASTER_PORT -b $MASTER_BLOCK_STORE -i $MASTER_PID $MASTER_ARGS"
WORKER_CMD="worker --logfile $LOGDIR/worker.log -D --port $WORKER_PORT -b $WORKER_BLOCK_STORE -i $WORKER_PID -m $MASTER_URI -n $WORKER_SLOTS $WORKER_ARGS"

#
# Function that starts the daemon/service
#
do_start()
{
	# Return
	#   0 if daemon has been started
	#   1 if daemon was already running
	#   2 if daemon could not be started
	if [ "$MASTER" = "yes" ]; then
	  start-stop-daemon --start --pidfile $MASTER_PID --exec $CMD -- $MASTER_CMD
	  RETVAL1="$?"
	fi
	if [ "$WORKER" = "yes" ]; then
	  start-stop-daemon --start --pidfile $WORKER_PID --exec $CMD -- $WORKER_CMD
	  RETVAL2="$?"
	fi
	[ "$RETVAL1" = 2 ] && return 2
	[ "$RETVAL2" = 2 ] && return 2
	[ "$RETVAL1" = 1 ] && return 1
	[ "$RETVAL2" = 1 ] && return 1
	return $RETVAL1
}

#
# Function that stops the daemon/service
#
do_stop()
{
	# Return
	#   0 if daemon has been stopped
	#   1 if daemon was already stopped
	#   2 if daemon could not be stopped
	#   other if a failure occurred
	if [ "$MASTER" = "yes" ]; then
	  start-stop-daemon --stop --quiet --retry=TERM/30/KILL/5 --pidfile $MASTER_PID 
	  RETVAL1="$?"
	  rm -f $MASTER_PID
	fi
	if [ "$WORKER" = "yes" ]; then
	  start-stop-daemon --stop --quiet --retry=TERM/30/KILL/5 --pidfile $WORKER_PID
	  RETVAL2="$?"
	  rm -f $WORKER_PID
	fi
	[ "$RETVAL1" = 2 ] && return 2
	[ "$RETVAL2" = 2 ] && return 2
	[ "$RETVAL1" = 1 ] && return 1
	[ "$RETVAL2" = 1 ] && return 1
	return $RETVAL1
}

#
# Function that sends a SIGHUP to the daemon/service
#
do_reload() {
	#
	# If the daemon can reload its configuration without
	# restarting (for example, when it is sent a SIGHUP),
	# then implement that here.
	#
	if [ "$MASTER" = "yes" ]; then
	  start-stop-daemon --stop --signal 1 --quiet --pidfile $MASTER_PID
	fi
	return 0
}

case "$1" in
  start)
    [ "$VERBOSE" != no ] && log_daemon_msg "Starting $DESC " "$NAME"
    do_start
    case "$?" in
		0|1) [ "$VERBOSE" != no ] && log_end_msg 0 ;;
		2) [ "$VERBOSE" != no ] && log_end_msg 1 ;;
	esac
  ;;
  stop)
	[ "$VERBOSE" != no ] && log_daemon_msg "Stopping $DESC" "$NAME"
	do_stop
	case "$?" in
		0|1) [ "$VERBOSE" != no ] && log_end_msg 0 ;;
		2) [ "$VERBOSE" != no ] && log_end_msg 1 ;;
	esac
	;;
  status)
       status_of_proc "$DAEMON" "$NAME" && exit 0 || exit $?
       ;;
  #reload|force-reload)
	#
	# If do_reload() is not implemented then leave this commented out
	# and leave 'force-reload' as an alias for 'restart'.
	#
	#log_daemon_msg "Reloading $DESC" "$NAME"
	#do_reload
	#log_end_msg $?
	#;;
  restart|force-reload)
	#
	# If the "reload" option is implemented then remove the
	# 'force-reload' alias
	#
	log_daemon_msg "Restarting $DESC" "$NAME"
	do_stop
	case "$?" in
	  0|1)
		do_start
		case "$?" in
			0) log_end_msg 0 ;;
			1) log_end_msg 1 ;; # Old process is still running
			*) log_end_msg 1 ;; # Failed to start
		esac
		;;
	  *)
	  	# Failed to stop
		log_end_msg 1
		;;
	esac
	;;
  *)
	#echo "Usage: $SCRIPTNAME {start|stop|restart|reload|force-reload}" >&2
	echo "Usage: $SCRIPTNAME {start|stop|status|restart|force-reload}" >&2
	exit 3
	;;
esac

:
