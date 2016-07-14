@echo off

pushd %0\..\..

set /p VERSION=<VERSION

java -cp dist\job-streamer-control-bus-%VERSION%.jar;"lib\*" clojure.main -m job-streamer.control-bus.main

pause
