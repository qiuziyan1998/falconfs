#!/usr/bin/env bash
#

yum install -y gcc gcc-c++ make tar wget diffutils findutils cmake zlib-devel openssl-devel leveldb-devel gflags-devel cppunit cppunit-devel hostname autoconf libtool libsysfs automake pkg-config readline-devel lz4-devel libzstd-devel python3-devel bison flex jsoncpp-devel gtest-devel gmock-devel fuse-devel maven libibverbs-devel rpm-build rpmdevtools ninja-build

# gcc-14
wget https://mirrors.ustc.edu.cn/gnu/gcc/gcc-14.2.0/gcc-14.2.0.tar.gz
tar zxvf gcc-14.2.0.tar.gz
cd gcc-14.2.0
sed -i 's|http://gcc.gnu.org/pub/gcc/infrastructure/|http://www.mirrorservice.org/sites/sourceware.org/pub/gcc/infrastructure/|' ./contrib/download_prerequisites
./contrib/download_prerequisites
./configure -v --enable-checking=release --enable-languages=c,c++ --disable-multilib --disable-bootstrap
make -j8
make install
rm -f /usr/bin/gcc /usr/bin/g++ /usr/bin/c++ /usr/lib64/libstdc++.so*
update-alternatives --install /usr/bin/gcc gcc /usr/local/bin/gcc 100
update-alternatives --install /usr/bin/g++ g++ /usr/local/bin/g++ 100
update-alternatives --install /usr/bin/c++ c++ /usr/local/bin/c++ 100
ln -sf /usr/local/lib64/libstdc++.so.6.0.33 /usr/lib64/libstdc++.so.6
ln -sf /usr/local/lib64/libstdc++.so.6.0.33 /usr/lib64/libstdc++.so
cd /

# third_party

# protobuf
wget https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.21.12.tar.gz
tar zxvf v3.21.12.tar.gz
cd protobuf-3.21.12
mkdir build
cd build
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_BUILD_SHARED_LIBS=ON ..
make -j8
make install
ldconfig
cd /
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64/

# glog
wget https://github.com/google/glog/archive/refs/tags/v0.6.0.tar.gz
tar zxvf v0.6.0.tar.gz
cd glog-0.6.0
mkdir build && cd build && cmake .. && make -j8 && make install
cd /

# brpc
wget https://github.com/apache/brpc/archive/refs/tags/1.12.1.tar.gz
tar zxvf 1.12.1.tar.gz
cd brpc-1.12.1
mkdir build && cd build && cmake -DWITH_GLOG=ON -DWITH_RDMA=ON .. && make -j8 && make install
cd /

# huaweicloud-sdk-c-obs
if [ $(uname -m) == "aarch64" ]; then
    export OBS_BUILD_SCRIPT=build_aarch.sh
else
    export OBS_BUILD_SCRIPT=build.sh
fi
wget https://github.com/huaweicloud/huaweicloud-sdk-c-obs/archive/refs/tags/v3.24.12.tar.gz
tar zxvf v3.24.12.tar.gz
cd huaweicloud-sdk-c-obs-3.24.12
sed -i '/if(NOT DEFINED OPENSSL_INC_DIR)/,+5d' CMakeLists.txt
sed -i '/OPENSSL/d' CMakeLists.txt
cd source/eSDK_OBS_API/eSDK_OBS_API_C++ && \
    export SPDLOG_VERSION=spdlog-1.12.0 && bash $OBS_BUILD_SCRIPT sdk && \
    mkdir -p /usr/local/obs && \
    tar zxvf sdk.tgz -C /usr/local/obs && \
    rm -f /usr/local/obs/lib/libcurl.so* && \
    rm -f /usr/local/obs/lib/libssl.so* && \
    rm -f /usr/local/obs/lib/libcrypto.so* && \
    ln -sf /usr/local/obs/lib/libiconv.so /usr/local/obs/lib/libiconv.so.0
cd /

# prometheus-cpp
yum install -y libcurl-devel
export PROMETHEUS_CPP_VERSION=1.3.0
export PROMETHEUS_DOWNLOAD_URL=https://github.com/jupp0r/prometheus-cpp/releases/download/v${PROMETHEUS_CPP_VERSION}/prometheus-cpp-with-submodules.tar.gz

wget -O- "${PROMETHEUS_DOWNLOAD_URL}" | tar -xzvf - -C /tmp &&
    cd "/tmp/prometheus-cpp-with-submodules" &&
    mkdir build && cd build &&
    cmake .. -DBUILD_SHARED_LIBS=ON -DENABLE_PULL=ON -DENABLE_COMPRESSION=OFF &&
    make -j$(nproc) && make install &&
    rm -rf "/tmp/prometheus-cpp-with-submodule"
cd /

# flatbuffers
wget https://github.com/google/flatbuffers/archive/refs/tags/v25.2.10.tar.gz
tar -zxvf v25.2.10.tar.gz
cd flatbuffers-25.2.10
mkdir build
cd build
cmake ..
make -j8
make install
cd /

#fmt
yum install -y git
git clone https://github.com/fmtlib/fmt.git
cd fmt
mkdir build
cd build
cmake ..
make -j8
make install
cd /

# zookeeper
wget https://github.com/apache/zookeeper/archive/refs/tags/release-3.9.3.tar.gz
tar zxvf release-3.9.3.tar.gz
cd zookeeper-release-3.9.3/zookeeper-jute
mvn clean install -DskipTests
cd ../zookeeper-client/zookeeper-client-c
mvn clean install -DskipTests
mkdir /usr/local/include/zookeeper/
cp -r generated/*.h /usr/local/include/zookeeper/
cp -r include/*.h /usr/local/include/zookeeper/
cp -d target/c/lib/libzookeeper* /usr/local/lib64/
cd /
