FROM semtech/mu-javascript-template:feature-node-18
LABEL maintainer="info@redpencil.io"
ENV SUDO_QUERY_RETRY="true"
ENV SUDO_QUERY_RETRY_FOR_HTTP_STATUS_CODES="404,500,503"
