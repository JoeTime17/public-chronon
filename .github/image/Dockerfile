FROM ubuntu:24.04

# build using command: docker build --progress=plain -f .github/image/Dockerfile -t chronon-base .

# Install necessary tools and Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    bash \
    ca-certificates \
    git \
    zstd \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    python3.12-venv

# java
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH=$PATH:$JAVA_HOME/bin

ENV PATH="/usr/local/bin:${PATH}"
ENV XDG_CACHE_HOME=/root/.cache
ENV COURSIER_CACHE=/root/.cache/coursier
ENV UV_CACHE_DIR=/root/.cache/uv

# thrift
ARG THRIFT_VERSION=0.21.0
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libboost-dev \
    libssl-dev \
    libevent-dev \
    bison \
    flex \
    autoconf \
    automake \
    libtool && \
    curl -LSs https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz -o thrift-${THRIFT_VERSION}.tar.gz && \
    tar -xzf thrift-${THRIFT_VERSION}.tar.gz && \
    cd thrift-${THRIFT_VERSION} && \
    ./configure --without-python --without-cpp --without-nodejs --without-java && \
    make && \
    make install && \
    cd .. && \
    rm -rf thrift-${THRIFT_VERSION} thrift-${THRIFT_VERSION}.tar.gz && \
    apt-get purge -y \
    build-essential \
    cmake \
    libboost-dev \
    libssl-dev \
    libevent-dev \
    bison \
    flex \
    autoconf \
    automake \
    libtool && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Install uv - fast Python package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"

# Upgrade pip and install some common Python packages
RUN pip3 install --break-system-packages pytest tox flake8 ruff

RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /usr/lib/jvm && \
    ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/default-jvm

# Verify installations
RUN java -version && \
    thrift -version && \
    python3 --version  && \
    pip3 --version && \
    git --version && \
    uv --version

# Set working directory
WORKDIR /app

ENV LANG=en_US.UTF-8

# Cmd to run when starting the container
CMD ["/bin/bash"]
