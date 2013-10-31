"use strict";
var util = require('util'),
    redis = require('redis'),
    Q = require('q');

function RedisBackend(config) {
    var self = this;
    var client;
    var connect = Q.defer();
    var cache = Q.defer();

    this.connect = function () {
        if (!self.connected) {
            client = redis.createClient(config.backendconf.redis.port || 6379, config.backendconf.redis.host || '127.0.0.1');
            client.on('connect', function () {
                self.connected = true;
                connect.resolve(client)
            });
        }
        return connect.promise;
    }

    this.wait = function (callback) {
        if (callback) {
            self.connect().done(callback);
        } else {
            return self.connect();
        }
    };

    this.client = function () {
        return client;
    };

    this.get = function (model, keys, callback, options) {
        self.connect().done(function () {
            var results = { };
            if (keys === '*') {
                client.hgetall(self.namespace + model, function (err, results) {
                    for (var member in results) {
                        results[member] = JSON.parse(results[member]);
                    }
                    callback(err, results);
                });
            } else {
                if (model === 'cache^') {
                    if (util.isArray(keys)) {
                        for (var ii = 0; ii < keys.length; ii++) {
                            keys[ii] = self.namespace + model + keys[ii];
                        }
                    } else {
                        keys = self.namespace + model + keys;
                    }
                    client.mget(keys, function (err, recs) {
                        if (util.isArray(keys)) {
                            var results = { };
                            for (var ii = 0; ii < keys.length; ii++) {
                                results[keys[ii]] = JSON.parse(recs[ii]);
                            }
                            callback(err, results);
                        } else if (recs && recs[0]) {
                            callback(err, JSON.parse(recs[0]));
                        } else {
                            callback(err, null);
                        }
                    });
                } else {
                    client.hmget(self.namespace + model, keys, function (err, recs) {
                        if (util.isArray(keys)) {
                            var results = { };
                            for (var ii = 0; ii < keys.length; ii++) {
                                results[keys[ii]] = JSON.parse(recs[ii]);
                            }
                            callback(err, results);
                        } else if (recs && recs[0]) {
                            callback(err, JSON.parse(recs[0]));
                        } else {
                            callback(err, null);
                        }
                    });
                }
            }
        }, function (err) { if (typeof callback === 'function') callback(err, null); });
    }; 

    this.set = function (model, key, object, callback, options) {
        self.connect().done(function () {
            if (options && options.expiration > 0) {
                client.set(self.namespace + model + key, JSON.stringify(object), function (err, res) {
                    client.expire(self.namespace + model + key, options.expiration);
                    if (typeof callback === 'function') callback(err, null);
                });
            } else {
                client.hset(self.namespace + model, key, JSON.stringify(object), function (err, res) {
                    if (typeof callback === 'function') callback(err, null);
                });
            }
        }, function(err) { if (typeof callback === 'function') callback(err, null); });
    };

    this.del = function (model, key, callback) {
        self.connect().done(function () {
            client.hdel(self.namespace + model, key, function (err, result) {
                if (typeof callback === 'function') callback(err, result);
            });
        }, function(err) { if (typeof callback === 'function') callback(err, null); });
    };

    self.cache = {
        init: function () {
        },
        get: function (keys, callback) {
            self.connect().done(function() {
                self.get('cache^', keys, callback);
            }, function(err) { callback(err, null); });
        },
        set: function (key, value, expiration, callback) {
            self.connect().done(function() {
                self.set('cache^', key, value, callback, { expiration: expiration });
            }, function(err) { callback(err, null); });
        }
    };

    /*self.media = {
        send: function (recordid, name, res) {
            self.connect().done(function () {
                var gs = new mongodb.GridStore(database, recordid + '/' + name, 'r');
                gs.open(function (err, gs) {
                    if (err) {
                        res.send(404);
                    } else {
                        gs.stream(true);
                        gs.pipe(res);
                    }
                });
            });
        },
        save: function (recordid, name, metadata, tmppath, callback) {
            self.connect().done(function () {
                var gs = new mongodb.GridStore(database, recordid + '/' + name, 'w', metadata);
                gs.open(function(err, gridStore) {
                    gridStore.writeFile(tmppath, function (err) {
                        fs.unlink(tmppath, function () {
                            callback(err);
                        });
                    });
                });
            });
        },
        del: function (recordid, name, callback) {
            self.connect().then(function () {
                mongodb.GridStore.unlink(database, recordid + '/' + name, callback);
            });
        }
    };

    self.dump = function (outstream, promise, wantedList) {
        self.connect().done(function () {
            database.collections(function (err, collections) {
                var wanted = { };
                var queue = [ ];
                if (wantedList) {
                    wantedList.forEach(function (col) {
                        wanted[col] = true;
                    });
                }
                var dumpCollection = function (col, repeat) {
                    if (repeat) outstream.write(",\n");
                    outstream.write('"' + col.collectionName + '": [');
                    var colstream = col.find().stream();
                    var firstChunk = true;
                    colstream.on('data', function (chunk) {
                        if (!firstChunk) outstream.write(",");
                        firstChunk = false;
                        outstream.write(JSON.stringify(chunk));
                    });
                    colstream.on('end', function () {
                        outstream.write("]");
                        if (queue.length > 0) {
                            dumpCollection(queue.shift(), true);
                        } else {
                            outstream.end("}\n", function () {
                                promise.resolve(true);
                            });
                        }
                    });
                };
                collections.forEach(function (col, index) {
                    if ((!wantedList || wanted[col.collectionName]) && col.collectionName !== 'system.indexes') {
                        queue.push(col);
                    }
                });
                if (queue.length > 0) {
                    outstream.write("{\n");
                    dumpCollection(queue.shift());
                } else {
                    promise.resolve(true);
                }
            });
        });
    };*/

    config.backendconf = config.backendconf || { };
    config.backendconf.redis = config.backendconf.redis || { };
    self.namespace = config.backendconf.redis.namespace || 'biblionarrator';
    self.namespace += '^';
    self.cacheexpire = config.cacheconf.defaultexpiry || 600;
    self.connected = false;
}

module.exports = RedisBackend;
