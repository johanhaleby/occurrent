#!/bin/bash
set -e
mkdir -p /data/cfg /data/sh /data/log
mongod --configsvr --replSet cfg --port 27019 --bind_ip_all --dbpath /data/cfg --fork --logpath /data/log/cfg.log
mongod --shardsvr --replSet sh --port 27018 --bind_ip_all --dbpath /data/sh --fork --logpath /data/log/sh.log
mongosh --quiet --port 27019 --eval 'rs.initiate({_id:"cfg",configsvr:true,members:[{_id:0,host:"localhost:27019"}]})'
mongosh --quiet --port 27018 --eval 'rs.initiate({_id:"sh",members:[{_id:0,host:"localhost:27018"}]})'
until mongosh --quiet --port 27019 --eval 'db.hello().isWritablePrimary' | grep -q true; do sleep 1; done
until mongosh --quiet --port 27018 --eval 'db.hello().isWritablePrimary' | grep -q true; do sleep 1; done
mongos --configdb cfg/localhost:27019 --port 27017 --bind_ip_all --fork --logpath /data/log/m1.log
mongos --configdb cfg/localhost:27019 --port 27020 --bind_ip_all --fork --logpath /data/log/m2.log
mongosh --quiet --port 27017 --eval 'sh.addShard("sh/localhost:27018")'
echo "SHARDED CLUSTER READY"
tail -f /dev/null
