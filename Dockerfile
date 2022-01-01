FROM python:3.7

LABEL maintainer="wmz669082@gmail.com"

WORKDIR /usr/src/app

COPY . /usr/src/app

ENV DISPLAY host.docker.internal:0.0

RUN apt-get update
# RUN apt-get -y install vim less cat sed awk echo cut lsof
RUN apt-get install -y --no-install-recommends build-essential gcc iproute2
RUN apt-get install ffmpeg libsm6 libxext6  -y

# pre-install numpy for ta-lib
RUN pip install numpy

# install ta-lib for python
# RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
#     tar xzvf ta-lib-0.4.0-src.tar.gz && \
#     cd ta-lib && \
#     ./configure --prefix=/usr && \
#     make && \
#     make install && \
#     # cd .. && \
#     # rm -rf ta-lib*
#     export TA_LIBRARY_PATH=/usr/lib && \
#     export TA_INCLUDE_PATH=/usr/include
# RUN pip install ta-lib

RUN tar xzvf ./files/ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    # cd .. && \
    # rm -rf ta-lib*
    export TA_LIBRARY_PATH=/usr/lib && \
    export TA_INCLUDE_PATH=/usr/include
RUN pip install ta-lib

# Windows server
# RUN pip install ./files/TA_Lib-0.4.22-cp37-cp37m-win_amd64.whl
# RUN pip install ./files/quickfix-1.15.1-cp37-cp37m-win_amd64.whl

RUN pip install -r ./requirements-manual.txt

ENV DOCKER_HOST "host.docker.internal"

# CMD ["python", "-m", "server.muz"]
ENTRYPOINT python -m server.muz
