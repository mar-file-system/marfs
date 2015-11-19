from centos:centos7

run yum -y update && \
    yum -y install gcc glibc-devel fuse-devel libattr-devel make && \
    yum -y install curl-devel curl openssl-devel openssl && \
    yum -y install git libxml2-devel && \
    yum -y clean all
run mkdir /app /app/aws4c /app/marfs /app/PAX2
run git clone https://github.com/jti-lanl/aws4c /app/aws4c
run git clone https://github.com/mar-file-system/marfs /app/marfs
run git clone https://github.com/mar-file-system/PA2X /app/PA2X
run make -C /app/PA2X base
run make -C /app/aws4c
run cd /app/marfs/common/configuration/src ; make PARSE_DIR=/app/PA2X
run make -C /app/marfs/fuse/src LIBAWS4C=/app/aws4c
