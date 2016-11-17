@echo off

pushd %0\..\..

set /p VERSION=<VERSION

java -cp %CONTROL_BUS_RESOURCE_PATH%;resouces;dist\job-streamer-control-bus-%VERSION%.jar;"lib\*" clojure.main -m job-streamer.control-bus.main

pause
