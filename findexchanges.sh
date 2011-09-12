#!/usr/bin/sh
fgrep "var _ticker =" ../googlehtml/*.html | sed "s/.*'\(.*\)'.*/\1/" > exchange.txt
