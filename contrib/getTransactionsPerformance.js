'use strict';

var WalletService = require('../lib/services/wallet-api');
var levelup = require('levelup');
var leveldown = require('leveldown');
var async = require('async');
var bitcore = require('bitcore-lib');
var fs = require('fs');
var Writable = require('stream').Writable;
var express = require('express');
var request = require('request');

var transactionCount = 1000000; // should be multiple of 1000

var app = express();
var server;

var db = {
  _store: levelup('./testData', { db: leveldown, keyEncoding: 'binary', valueEncoding: 'binary'})
};

db.getPrefix = function(name, callback) {
  setImmediate(function() {
    callback(null, new Buffer('0000', 'hex'));
  });
};

db.pauseSync = function(callback) {
  setImmediate(callback);
};

db.resumeSync = function() {

};

db.batch = function(ops, options, callback) {
  var cb = callback;
  var opts = options;
  if (typeof callback !== 'function') {
    cb = options;
    opts = {};
  }
  if (!this._stopping) {
    this._store.batch(ops, opts, cb);
  } else {
    setImmediate(cb);
  }
};

db.createReadStream = function(op) {
  var stream;
  if (!this._stopping) {
    stream = this._store.createReadStream(op);
  }
  return stream;
};

db.createKeyStream = function(op) {
  var stream;
  if (!this._stopping) {
    stream = this._store.createKeyStream(op);
  }
  return stream;
};

var transaction = {};
transaction.getTransaction = function(txid, options, callback) {
var rawTx = '010000000196dbae7b9f523e599ff1197be34863d9986fd491767f6846193420ce5fcdaa20000000006b483045022100bc2f5f5e3736483bb939078eecfcbaf2b27aee5a6874819b1ee62e405e67ecc702201ed476ba29f4bd8fab9367f19a2155159d6450f2f9fde539127539945bb62aad012102849c38d98963a1ed4a32134a80b3410fb689556799d433363356ba6a244d92b9ffffffff02405a6659000000001976a91411a325ec6583557b535b23dde25c7891879b6ffb88ac80b40395000000001976a91448ce935003e6139ca6ec83f2cf36d2cdf0410a4b88ac00000000';
var tx = new bitcore.Transaction(rawTx);
  setImmediate(function() {
    callback(null, tx);
  });
};

var node = {
  services: {
    db: db,
    transaction: transaction
  }
};

var options = {
  node: node
};

var service = new WalletService(options);

function startService(callback) {
  console.log('Starting service...');
  service.start(callback);
}

function writeValues(callback) {
  console.log('Writing values...');

  var txidBuffer = Buffer.alloc(32);

  async.timesSeries(transactionCount / 1000, function(i, next) {
    var ops = [];

    for(var j = 0; j < 1000; j++) {
      var num = (i * 1000) + j + 1;
      txidBuffer.writeUInt32LE(num);
      ops.push(
        {
          type: 'put',
          key: service._encoding.encodeWalletTransactionKey('wallet0', num, txidBuffer.toString('hex'))
        }
      );
    }

    service.db.batch(ops, next);
  }, callback);
}

function setupExpress(callback) {
  app.get('/wallets/:walletId/transactions', function(req, res) {
    var fn = service._endpointGetTransactions();
    fn(req, res);
  });

  server = app.listen(3123, function () {
    console.log('Listening on port 3123');
    callback();
  });
}

function getTransactions(callback) {
  var stream = request('http://localhost:3123/wallets/wallet0/transactions');
  var writeStream = new Writable();
  var count = 0;
  writeStream._write = function(chunk, enc, callback) {
    if(count % (transactionCount / 100) === 0) {
      process.stdout.write('.');
    }
    count++;
    // swallow output
    setImmediate(callback);
  };
  stream.pipe(writeStream);
  stream.on('end', callback);
}

async.series(
  [
    startService,
    writeValues,
    setupExpress,
    getTransactions
  ], function(err) {
    if(err) {
      console.log(err);
    }

    console.log('done');
    server.close();
  }
);

