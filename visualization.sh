#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd "$DIR/Flask_app"

export FLASK_APP="controller.py"

# En desarrollo
flask run &

sleep 2

xdg-open http://127.0.0.1:5000
wait
