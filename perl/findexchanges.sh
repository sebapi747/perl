#!/usr/bin/sh
fgrep "var _ticker =" googlehtml/* | sed "s/.*'\(.*\)'.*/\1/" > exchange.txt
