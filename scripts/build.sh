export SDE=~/bf-sde-9.9.0
export SDE_INSTALL=$SDE/install
export PATH=$SDE_INSTALL/bin:$PATH

sudo rm -rf *.log
sudo rm -rf build
mkdir build && cd build 

cmake $SDE/p4studio/ \
 -DCMAKE_INSTALL_PREFIX=$SDE/install \
 -DCMAKE_MODULE_PATH=$SDE/cmake \
 -DP4_NAME=splitter \
 -DP4_PATH=path/to/splitter-switch/p4src/splitter.p4 

sudo make splitter && make install
