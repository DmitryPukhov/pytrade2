##############################################
# Build base image, rarely changed
##############################################
FROM python:3.11-slim as builder

# Install system dependencies first (including lightgbm requirements)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /pytrade2

# First copy only requirements to leverage Docker cache
COPY pyproject.toml .

# Create a virtual environment and install dependencies
RUN mkdir -p pytrade2 && python -m venv \
    /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir .

# Install libs
COPY pyproject.toml .
RUN pip install --no-cache-dir .

##############################################
# Create a runtime image, often changed
##############################################
FROM python:3.11-slim

# Install runtime system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder
COPY --from=builder /opt/venv /opt/venv

# set the working directory in the container
WORKDIR /pytrade2

# copy the content of the local src directory to the working directory
COPY pytrade2/ .
# Ensure scripts in virtual environment are executable
ENV PATH="/opt/venv/bin:$PATH"

# Remove dev config
RUN rm -f ./cfg/app-dev.yaml

# command to run on container start
CMD [ "python", "./App.py" ]