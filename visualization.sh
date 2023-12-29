#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd "$DIR/Flask_app"

flask run &
sleep 2

xdg-open http://127.0.0.1:5000
wait