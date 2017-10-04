#
# JobStreamer control-bus
#
FROM openjdk:8-jdk

RUN mkdir -p /opt/job-streamer
ARG version
ADD https://github.com/job-streamer/job-streamer-control-bus/releases/download/v${version}/job-streamer-control-bus-${version}-dist.zip /opt/job-streamer/
RUN unzip /opt/job-streamer/job-streamer-control-bus-${version}-dist.zip -d /opt/job-streamer/
RUN rm -f /opt/job-streamer/job-streamer-control-bus-${version}-dist.zip
CMD [ "sh", "-c", "cd /opt/job-streamer/job-streamer-control-bus-${version} && bin/control_bus" ]