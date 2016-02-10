FROM  golang:1.5.3

RUN mkdir -p /tools/snappy \
 && (cd /tools; curl -L https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz | tar xzf -) \
 && (cd /tools/snappy-1.1.3; ./configure; make install) \
 && mkdir -p /tools/rocks \
 && (cd /tools; curl -L https://github.com/facebook/rocksdb/archive/v4.2.tar.gz | tar xzf -) \
 && (cd /tools/rocksdb-4.2; make install-shared) \
 && go get github.com/tools/godep \
 && rm -rf /tools
