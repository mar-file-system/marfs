#!/bin/bash -ex

function git_submodule_init()
{
    grep -q '^.submodule' .git/config || \
    git submodule init
}

function git_modules_aws4c()
{
    if ! grep -q '^.submodule "aws4c"' .git/config ; then
        # repo=git@github.com:archive-engines/aws4c.git
        repo=https://github.com/jti-lanl/aws4c.git
        git submodule add $repo
        (cd aws4c && git checkout lanl)
    else
        git submodule update aws4c 
        # (cd aws4c ; git pull)
    fi
}

function git_modules_PA2X()
{
    if ! grep -q '^.submodule "PA2X"' .git/config ; then
        # repo=git@github.com:archive-engines/PA2X.git
        repo=https://github.com/mar-file-system/PA2X.git
        git submodule add $repo
        (cd PA2X && git checkout master)
    else
        git submodule update PA2X
        # (cd PA2X ; git pull)
    fi
}

git_submodule_init
git_modules_aws4c
git_modules_PA2X

autoreconf -i --force

# export DEBUG=0
PARSE_DIR=`pwd`/PA2X/ \
    AWS4C=`pwd`/aws4c/ \
    ./configure --prefix=/usr --enable-debug --without-aws-auth

make -j3 -C aws4c lib lib_extra
make -j3
