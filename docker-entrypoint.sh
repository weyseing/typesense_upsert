#!/bin/sh

# workdir
cd /app

# start cron
printenv >> /etc/environment # env for cron service
crontab /app/cron/crontab
service cron start

python manage.py runserver 0.0.0.0:8000             
# tail -f /dev/null