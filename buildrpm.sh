#!/bin/bash
set -ex

function archive_aws4c_src()
{
    pushd aws4c
    eval `git describe --tags --abbrev=4 | ( IFS=- read ver rev id ;  echo "ver=$ver rev=$rev id=$id" )`
    sver=${ver#marfs_}
    git archive --format=tar --prefix=aws4c/ HEAD |\
        gzip > ~/rpmbuild/SOURCES/marfs-aws4c-${sver}.tar.gz
    popd
}

function archive_PA2Xc_src()
{
    pushd PA2X
    eval `git describe --tags --abbrev=4 | ( IFS=- read ver rev id ;  echo "ver=$ver rev=$rev id=$id" )`
    sver=${ver#marfs_}
    git archive --format=tar --prefix=PA2X/ HEAD |\
        gzip > ~/rpmbuild/SOURCES/marfs-PA2X-${sver}.tar.gz
    popd
}


function build_marfs_rpm()
{
    #ver=v1.6.0  # FIXME
    eval `git describe --tags --abbrev=4 | ( IFS=- read ver rev id ;  echo "ver=$ver rev=$rev id=$id" )`
    sver=${ver#marfs_}
    git archive --format=tar --prefix=marfs-${sver}-$rev-$id/ HEAD |\
        gzip > ~/rpmbuild/SOURCES/marfs-${sver}.tar.gz
    sed -e 's/%{marfs_version}/'$sver'/; s/%{_marfs_rev}/'${rev}'/; s/%{_marfs_id}/'${id}'/' \
        < marfs.spec.in > marfs.spec
    rpmbuild -D "marfs_version ${sver}" -D "_marfs_rev ${rev}" -D "_marfs_id ${id}" -ba marfs.spec 
}

archive_aws4c_src
archive_PA2Xc_src
build_marfs_rpm
