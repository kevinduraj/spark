#!/usr/bin/bash

echo "show status like 'open%'" | mysql --login-path=local -t -N | while read -r line
do
    echo "$line"
done

