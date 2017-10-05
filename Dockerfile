FROM openjdk:8-jdk

ENV LEIN_ROOT 1

RUN curl -L -s https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > \
   /usr/local/bin/lein \
 && chmod 0755 /usr/local/bin/lein \
 && lein upgrade

RUN mkdir -p /opt/job-streamer/job-streamer-control-bus
WORKDIR /opt/job-streamer/job-streamer-control-bus
ADD ./ ./

RUN lein deps
CMD lein run