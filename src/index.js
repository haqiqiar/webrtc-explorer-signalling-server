var Hapi = require('hapi');
var io = require('socket.io');
var config = require('config');
var Id = require('dht-id');

var server = new Hapi.Server(config.get('hapi.options'));

var numberOfResourcePeers = 5;

server.connection({
    port: config.get('hapi.port')
});

server.route({
    method: 'GET',
    path: '/',
    handler: function(request, reply) {
        reply('Signaling Server');
    }
});

server.route({
    method: 'GET',
    path: '/dht',
    handler: function(request, reply) {
        reply(peers);
    }
});


server.start(started);

function started() {
    io.listen(server.listener).on('connection', ioHandler);
    console.log('Signaling server has started on:', server.info.uri);
}

var peers = {};
var resourcePeers = {};
// id : {
//     socketId:
//     fingerTable: {
//         row : {
//             ideal: <fingerId>
//             current: <fingerId>
//         }
//     }
//     predecessorId
// }

var sockets = {};
var pendingSocketOperations = {};
var clientFingerTables = {};
// socketId: socket

function ioHandler(socket) {
    var peerId;
    // signalling server interactions
    socket.on('s-register', registerPeer);
    socket.on('s-unregister', unregisterPeer);
    socket.on('disconnect', peerRemove); // socket.io own event
    socket.on('s-send-offer', sendOffer);
    socket.on('s-offer-accepted', offerAccepted);

    // updates the availability or non-availability of this peer as a computing resource
    // {
    //  provideResources : [true|false]
    // }
    socket.on('update-resource-state', updateResourceState);

    // Retrieves an arbitrary number of resource peers in the following format:
    // {
    //   peers : [id1, id2, id3,...]
    // }
    socket.on('get-resource-peers', getResourcePeers);

    function registerPeer(data) {
        console.log("Register peer ", data);
        peerId = new Id(Id.hash(data.id));
        peers[peerId.toHex()] = {
            socketId:  socket.id,
            fingerTable: {}
        };

        sockets[socket.id] = socket;

        socket.emit('c-registered', {peerId: peerId.toHex()});

        console.log('registered new peer: %s(%s)', peerId.toHex(), data.id);

        calculateIdealFingers(peerId);
        updateFingers();
    }

    function unregisterPeer(data) {
        Object.keys(peers).map(function(peerId) {
            if (peers[peerId].socketId === socket.id) {
                delete peers[peerId];
                delete sockets[socket.id];
                delete pendingSocketOperations[socket.id];
                console.log('peer with Id: %s has disconnected', peerId);
            }
        });

        updateFingers();
    }


    function calculateIdealFingers(peerId) {
        //var fingers = config.get('explorer.fingers');
        var k = 1;
        //while (k <= fingers.length) {
        //    var ideal = (peerId.toDec() + Math.pow(2, fingers[k - 1])) %
        //Math.pow(2, 48);
        while (k <= config.get('explorer.n-fingers')) {
            var ideal = (peerId.toDec() + Math.pow(2, k)) % Math.pow(2, 48);
            peers[peerId.toHex()].fingerTable[k] = {
                ideal: new Id(ideal).toHex(),
                current: undefined
            };
            k++;
        }
    }

    function updateFingers() {
        if (Object.keys(peers).length < 2) {
            var myPeer = peers[Object.keys(peers)[0]];
            if(myPeer) {
                delete myPeer.predecessorId;
                Object.keys(myPeer.fingerTable).forEach(function (rowIndex) {
                    delete myPeer.fingerTable[rowIndex].current;
                });
            }
            return;
        }

        var sortedPeersId = Object.keys(peers).sort(function(a, b) {
            var aId = new Id(a);
            var bId = new Id(b);
            if (aId.toDec() > bId.toDec()) {
                return 1;
            }
            if (aId.toDec() < bId.toDec()) {
                return -1;
            }
            if (aId.toDec() === bId.toDec()) {
                console.log('error - There should never two identical ids');
                process.exit(1);
            }
        });

        sortedPeersId.forEach(function(peerId) {

            // predecessor
            var predecessorId = predecessorTo(peerId, sortedPeersId);

            if (peers[peerId].predecessorId !== predecessorId) {
                peers[peerId].predecessorId = predecessorId;

                queueSocketOperation(peers[peerId].socketId,'c-predecessor', {
                    predecessorId: predecessorId
                });
                //sockets[peers[peerId].socketId].emit('c-predecessor', {
                //    predecessorId: predecessorId
                //});
            }

            // successors
            Object.keys(peers[peerId].fingerTable).some(function(rowIndex) {
                var fingerId = sucessorTo(peers[peerId]
                                    .fingerTable[rowIndex]
                                    .ideal, sortedPeersId);

                if (peers[peerId].fingerTable[rowIndex].current !==
                    fingerId) {

                    peers[peerId].fingerTable[rowIndex].current = fingerId;

                    queueSocketOperation(peers[peerId].socketId,'c-finger-update', {
                            rowIndex: rowIndex,
                            fingerId: fingerId
                        });
                    //sockets[peers[peerId].socketId].emit('c-finger-update', {
                    //    rowIndex: rowIndex,
                    //    fingerId: fingerId
                    //});
                }

                if (Object.keys(peers).length <
                        config.get('explorer.min-peers')) {
                    return true; // stops the loop, calculates only
                    // for the first position (aka sucessor of the node
                }
            });
        });

        function queueSocketOperation(socketId, event, data){
            if(!(socketId in pendingSocketOperations)){
                pendingSocketOperations[socketId] = [];
            }


            pendingSocketOperations[socketId].push({ev:event, data:data});
            if(pendingSocketOperations[socketId].length == 1) {
                sendNow();
            }

            function sendNow(){
                sockets[socketId].emit(pendingSocketOperations[socketId][0].ev, pendingSocketOperations[socketId][0].data, function(ack){
                    pendingSocketOperations[socketId].shift();
                    if(pendingSocketOperations[socketId].length > 0){
                        sendNow();
                    }
                });
            }
        }

        function sucessorTo(pretendedId, sortedIdList) {
            pretendedId = new Id(pretendedId).toDec();
            sortedIdList = sortedIdList.map(function(inHex) {
                return new Id(inHex).toDec();
            });

            var sucessorId;
            sortedIdList.some(function(value, index) {
                if (pretendedId === value) {
                    sucessorId = value;
                    return true;
                }

                if (pretendedId < value) {
                    sucessorId = value;
                    return true;
                }

                if (index + 1 === sortedIdList.length) {

                    sucessorId = sortedIdList[0];
                    return true;
                }
            });

            return new Id(sucessorId).toHex();
        }

        function predecessorTo(peerId, sortedIdList) {
            var index = sortedIdList.indexOf(peerId);

            var predecessorId;

            if (index === 0) {
                predecessorId = sortedIdList[sortedIdList.length - 1];
            } else {
                predecessorId = sortedIdList[index - 1];
            }

            return new Id(predecessorId).toHex();
        }
    }

    function peerRemove() {
        Object.keys(peers).map(function(peerId) {
            if (peers[peerId].socketId === socket.id) {
                delete peers[peerId];
                delete sockets[socket.id];
                delete pendingSocketOperations[socket.id];
                console.log('peer with Id: %s has disconnected', peerId);
            }
        });

        updateFingers();
    }

    // signalling mediation between two peers

    function sendOffer(data) {
        console.log('Sending as offerer %s', JSON.stringify(data));
        sockets[peers[data.offer.dstId].socketId]
            .emit('c-accept-offer', data);
    }

    function offerAccepted(data) {
        console.log('Sending as answerer %s', JSON.stringify(data));
        sockets[peers[data.offer.srcId].socketId]
            .emit('c-offer-accepted', data);
    }


    function updateResourceState(data) {
        if(data.provideResources){
            console.log("Peer '%s' is now providing resources", peerId.toHex());
            resourcePeers[peerId.toHex()] = peers[peerId.toHex()];
        } else {
            console.log("Peer '%s' is no longer providing resources", peerId.toHex());
            delete resourcePeers[peerId.toHex()];
        }
    }

    function getResourcePeers(data, cb){
        var keys = Object.keys(resourcePeers);

        var discoveredPeers = [];

        for(var i = 0; i<Math.min(keys.length, numberOfResourcePeers); i++) {
            var p = keys[Math.floor(keys.length * Math.random())];

            if(discoveredPeers.indexOf(p) >= 0 || p === peerId) {
                i--;
            } else {
                discoveredPeers.push(p);
            }
        }

        cb(discoveredPeers);
    }

}
