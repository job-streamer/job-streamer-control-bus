#
# JobStreamer control-bus
#
FROM openjdk:8-jdk

RUN mkdir -p /opt/job-streamer
ADD https://github.com/job-streamer/job-streamer-control-bus/releases/download/v1.2.0/job-streamer-control-bus-1.2.0-dist.zip /opt/job-streamer/
RUN unzip /opt/job-streamer/job-streamer-control-bus-1.2.0-dist.zip -d /opt/job-streamer/
RUN rm -f /opt/job-streamer/job-streamer-control-bus-1.2.0-dist.zip
CMD [ "sh", "-c", "cd /opt/job-streamer/job-streamer-control-bus-1.2.0 && bin/control_bus" ]