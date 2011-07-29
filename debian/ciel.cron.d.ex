#
# Regular cron jobs for the ciel package
#
0 4	* * *	root	[ -x /usr/bin/ciel_maintenance ] && /usr/bin/ciel_maintenance
