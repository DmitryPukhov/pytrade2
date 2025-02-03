# set base image (host OS)
FROM python:3.11

# set the working directory in the container
WORKDIR /pytrade2

# copy the content of the local src directory to the working directory
COPY pytrade2/ .

WORKDIR /
# Install libs
COPY pyproject.toml .
RUN pip install .

WORKDIR /pytrade2
RUN rm -f ./cfg/app-dev.yaml
# command to run on container start
CMD [ "python", "./App.py" ]