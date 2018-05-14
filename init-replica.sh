echo "killing mongod and mongos"
pkill mongod

echo "removing data files"
rm -rf ~/data/config
rm -rf ~/data/rs*
rm -rf ~/data/logs

mkdir -p ~/data/rs0 ~/data/rs1 ~/data/rs2 ~/data/logs

LOGPATH=$(cd && pwd )/data/logs

# launch replSet s0

~/mongodb/mongodb-osx-x86_64-3.7.7/bin/mongod --replSet s0 --logpath "${LOGPATH}/r0.log" --dbpath ~/data/rs0 --port 27017 --fork --smallfiles
~/mongodb/mongodb-osx-x86_64-3.7.7/bin/mongod --replSet s0 --logpath "${LOGPATH}/r1.log" --dbpath ~/data/rs1 --port 27018 --fork --smallfiles
~/mongodb/mongodb-osx-x86_64-3.7.7/bin/mongod --replSet s0 --logpath "${LOGPATH}/r2.log" --dbpath ~/data/rs2 --port 27019 --fork --smallfiles

sleep 5

# connect to one server and initiate the set
~/mongodb/mongodb-osx-x86_64-3.7.7/bin/mongo --port 27017 << 'EOF'
config = { _id: "s0", members:[
{ _id : 0, host : "localhost:27017" },
{ _id : 1, host : "localhost:27018" },
{ _id : 2, host : "localhost:27019" }]};
rs.initiate(config)
EOF