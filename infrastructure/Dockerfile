FROM ubuntu:latest

RUN apt-get -y update && \
    apt-get -y install gcc git wget build-essential \
    libx11-dev libssl-dev \
    openssl libssl-dev libcurl4-openssl-dev \
    libcurl4-openssl-dev pkg-config \
    parallel

# Get library requirements to compile (Tcl and Tk)
RUN echo "Building degrib GUI requirements and prepare for build" && \
    mkdir -p /opt/degrib/tcltk && cd /opt/degrib/tcltk && \
    wget ftp://ftp.tcl.tk/pub/tcl/tcl8_6/tcl8.6.9-src.tar.gz && \
    wget ftp://ftp.tcl.tk/pub/tcl/tcl8_6/tk8.6.9-src.tar.gz && \
    wget https://core.tcl.tk/tcltls/uv/tcltls-1.7.16.tar.gz 

# Decompress and compile sources
RUN echo "Unzip sourcefiles and make installers" && \
    echo "Compiling Tcl ..." && \
    cd /opt/degrib/tcltk && \
    gunzip -c tcl8.6.9-src.tar.gz | tar -xf -&& \
    gunzip -c tk8.6.9-src.tar.gz | tar -xf -&& \
    cd tcl8.6.9/unix && \
    ./configure --prefix=/opt/degrib/tcltk && \
    make && \
    make install 

RUN  echo "Compiling Tk ..." && \
    cd /opt/degrib/tcltk && \
    gunzip -c tk8.6.9-src.tar.gz | tar -xf - && \
    cd tk8.6.9/unix && \
    ./configure --prefix=/opt/degrib/tcltk && \
    make && \
    make install

RUN echo "Compiling Tcltls ..."  && \
    cd /opt/degrib/tcltk && \
    gunzip -c tcltls-1.7.16.tar.gz | tar -xf -&& \
    cd tcltls-1.7.16 && \
    ./configure --prefix=/opt/degrib/tcltk --with-tcl=/opt/degrib/tcltk/lib && \
    make && \
    make install tls

# Download and install degrib
RUN echo "Downloading degrib ..." && \ 
    cd /opt/ && \
    echo "Downloading and extracting NWS's degrib" && \
    wget https://sats.nws.noaa.gov/~degrib/download/degrib-src.tar.gz && \
    gunzip -c degrib-src.tar.gz | tar -xf - 

RUN echo "Sourcing Makefiles and compiling degrib binaries" && \ 
    cd /opt/degrib/src/ && \
    ./configure --prefix=/opt/degrib TCL_PREFIX=/opt/degrib/tcltk TK_VERSION=8.6 TCL_VERSION=8.6&& \
    cd /opt/degrib/src/degrib && \
    sed '20,24s/^/#/' Makefile > Makefile_test && \
    rm Makefile && mv Makefile_test Makefile && \ 
    cd .. && \
    make && \
    cp -r /opt/degrib/tcltk/lib/tcltls1.7.16/ /opt/degrib/lib/

RUN echo "Export degrib to system PATH :-)"
ENV PATH="/opt/degrib/bin/:${PATH}"

