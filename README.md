**tmpbft测试**


## 1.make configtxgen

make configtxgen

## 2.make docker image

make orderer-docker-clean
make orderer-docker
docker tag hyperledger/fabric-orderer hyperledger/fabric-orderer:amd64-1.4.0

## 3.生成tendermint配置文件

可以通过go get -u https://github.com/tendermint/tendermint.git 安装tendermint可执行文件，仿照./examples/tmpbft-first-network/tmconfigs的目录规则手动生成配置，注意要全部配置成
validator节点。
也可以直接复制./examples/tmpbft-first-network/tmconfigs到本机根目录,  cp ./examples/tmpbft-first-network/tmconfigs ~/

## 4.启动first network网络
cd ./examples/tmpbft-first-network
sh byfn.sh up -i 1.4.0

##注意：
源代码中使用的viper版本有bug，需要升级到最新版本。且最新版本中也有bug，已经有人给出merge request，但是还未合并，所以作者手动做了代码修改。详情见
issue: https://github.com/spf13/viper/issues/373
merge request: https://github.com/spf13/viper/pull/545