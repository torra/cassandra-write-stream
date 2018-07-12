'use strict';

let stream = require('stream');

let cassandra = require('cassandra-driver');
let BB = require('bluebird');

/**

*/
class CassandraWriteStream extends stream.Writable {

  constructor(client_options, insert_query, query_options={}, row_transformer=(row) => row) {
    super();
    this._cassandra_client = BB.promisifyAll(new cassandra.Client(client_options));
    this._is_first = true;
    this._field_names = null;
    this._last_partial = '';
    this._insert_query = insert_query;
    this._promises = [];
    this._buffered_rows = [];
    this._query_options = Object.assign({}, query_options);
    this._row_transformer = row_transformer;
    this._calc_max_in_flight();
  }
  
  _calc_max_in_flight() {
    //https://docs.datastax.com/en/developer/nodejs-driver/3.5/features/connection-pooling/#simultaneous-requests-per-connection
    let state = this._cassandra_client.getState();
    this._max_in_flight = Math.max(state.getConnectedHosts().length * 2048, 2048);
  }

  _do_cassandra_write(row_data) {
    return this._cassandra_client.executeAsync(
      this._insert_query,
      row_data,
      this._query_options
    ).catch((err) => {
      this.emit('error', new Error(`Error writing to cassandra: ${err.message}`));
    });
  }

  _cleanup_fulfilled_writes() {
    //let's clean up the settled promises, because we don't need to keep track
    //of those any more
    this._promises = this._promises.filter(promise => promise.isPending());
  }

  _check_and_flush_buffer() {
    this._cleanup_fulfilled_writes();
    if(this._buffered_rows.length === 0) return;
    while(this._promises.length < this._max_in_flight && this._buffered_rows.length > 0) {
      let row_write = this._do_cassandra_write(this._buffered_rows.shift());
      this._promises.push(row_write);
      row_write.then(() => this._check_and_flush_buffer());
    }
  }

  _buffer_and_write(row_data) {
    this._cleanup_fulfilled_writes();
    this._calc_max_in_flight();
    if(this._promises.length < this._max_in_flight) {
      //NOTE we are assuming the first column is product_id!
      let row_promise = this._do_cassandra_write(row_data);
      this._promises.push(row_promise);
      row_promise.then(() => this._check_and_flush_buffer());
    } else {
      this._buffered_rows.push(row_data);
    }
  }

  //called by `stream.Writable`, must be implemented
  _write(chunk, enc, next) {
    //by splitting on newline, we see how many complete lines we have
    chunk = chunk.toString().split('\n');

    //we need to grab any partial line from the last _write() and combine w/
    //the first row of this current _write()
    chunk[0] = this._last_partial + chunk[0];
    this._last_partial = [];

    //now we can iterate over the rows
    while(chunk.length > 0) {
      let row = chunk.shift().split('\t');

      if(this._is_first) {//TODO can we get rid of this somehow?
        //the first row is the field names
        this._is_first = false;
        this._field_names = row;
        // what happens if there is a bug and the rows are all short a field?

      } else if(row.length === this._field_names.length) {
        // this means it is a full row, so we will write it to cassandra
        this._buffer_and_write(this._row_transformer(row, this._field_names));
      } else {
        //this means it is a partial row, so we'll buffer it for the next _write()
        this._last_partial = this._last_partial + row.join('\t');
      }
    }
    next();
  }

  _all_resolved() {
      return new Promise((res, rej) => {
          Promise.all(this._promises).then(() => {
              if(this._buffered_rows.length > 0) {
                  this._all_resolved().then(res, rej);
              } else {
                  res();
              }
          });
      });
  }

  //called by `stream.Writable`, used to signal when this stream is done writing
  _final(cb) {
    //let the remaining pending promises resolve
    this._all_resolved().then(() => {
        cb();
    }).catch((err) => {
        this.emit('error', new Error('Failed streaming to cassandra:' ,err));
    });
  }
}

module.exports = CassandraWriteStream;
