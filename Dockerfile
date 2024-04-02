# Pack library in a tarball
FROM node:alpine as builder
WORKDIR /opt/builder
COPY . .
RUN npm pack --pack-destination . \
    # rename file to something that can be called from the next stage
    && mv *.tgz kafkajs-throttled.tgz

ARG NODERED_VERSION=
FROM nodered/node-red:${NODERED_VERSION:-latest}

# Import tarball
COPY --from=builder /opt/builder/kafkajs-throttled.tgz /tmp/module/kafkajs-throttled.tgz
# installation requires root access
USER root
RUN chmod -R 777 /tmp/module
RUN npm i -S /tmp/module/kafkajs-throttled.tgz

# back to default user for runtime
USER node-red

