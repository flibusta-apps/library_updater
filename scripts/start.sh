#! /usr/bin/env sh

cd /app

/env.sh > ./.env

exec /usr/local/bin/library_updater