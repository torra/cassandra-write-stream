'use strict';

describe('CassandraWriteStream', function(){
  let CassandraWriteStream,
      cc,
      test_keyspace_name = 'cassy_write_stream_test_ks',
      expect,
      
      BB;
      
  this.timeout(10000);
  
  before(function(){
    expect = require('chai').expect;
    CassandraWriteStream = require('./');
    let cassandra = require('cassandra-driver');
    BB = require('bluebird');
    cc = BB.promisifyAll(new cassandra.Client({ contactPoints: ['127.0.0.1', '127.0.0.2']}));
    return cc.executeAsync(`create keyspace ${test_keyspace_name} with replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}`, null, {consistency: cassandra.types.consistencies.all})
    .then(() => {
      cc = BB.promisifyAll(new cassandra.Client({ contactPoints: ['127.0.0.1', '127.0.0.2'], keyspace: test_keyspace_name}));
      let data_create = cc.executeAsync(`CREATE TABLE IF NOT EXISTS data( product_id text, field text, value text, primary key (product_id) )`);
      let data2_create = cc.executeAsync(`CREATE TABLE IF NOT EXISTS data2( product_id text, details map<text,text>, primary key (product_id) )`);
      return BB.all([data_create, data2_create]);
    });
  });
  
  after(function(){
    return cc.executeAsync(`drop keyspace ${test_keyspace_name}`).catch(function(err){
      console.log(err);
    });
  });
  
  function _promise_for_stream(cassy_stream, sample_data_file) {
    return new Promise(function(res, rej){
      let finish_handler = () => res();
      let error_handler = function(err) {
        this.removeListener('finish', finish_handler);
        this.removeListener('error', error_handler);
        rej(err);
      };
      
      let Readable = require('stream').Readable;

      let s = new Readable();
      s.pipe(cassy_stream)
      .on('finish', finish_handler)
      .on('error', error_handler);
      
      s.push(sample_data_file);
      s.push(null);
    });
  }
  
  it('should stream data using the default row transformer', function(){
    let sample_data_file = [
      ['product_id', 'field', 'value'].join('\t'),
      ['pid1', 'color', 'orange'].join('\t'),
      ['pid2', 'color', 'blue'].join('\t')
    ].join('\n');
    
    let cassy_stream = new CassandraWriteStream(
      {contactPoints: ['127.0.0.1', '127.0.0.2'], keyspace: test_keyspace_name},
      `INSERT INTO data (product_id, field, value) 
        VALUES (?, ?, ?) USING TTL ${1000}`,
      {prepare: true, isIdempotent: true}
    );
    
    return _promise_for_stream(cassy_stream, sample_data_file)
    .then(function(){
      return cc.executeAsync(`select * from data where product_id='pid1'`, {prepare: true});
    }).then(function(results){
      expect(results.rows).to.have.length(1);
      expect(results.rows[0].product_id).to.eql('pid1');
      expect(results.rows[0].field).to.eql('color');
      expect(results.rows[0].value).to.eql('orange');
    });
  });
  
  it('should stream data in to cassandra and let us know when its done', function(){
    //create some test data
    let sample_data_file = [
      ['product_id', 'category_list', 'color'].join('\t'),
      ['pid1', 'cat1|cat2', 'blue'].join('\t')
    ].join('\n');

    let cassy_stream = new CassandraWriteStream(
      {contactPoints: ['127.0.0.1', '127.0.0.2'], keyspace: test_keyspace_name},
      `INSERT INTO data2 (product_id, details) 
        VALUES (?, ?) USING TTL ${1000}`, 
      {prepare: true, isIdempotent: true},
      function(row, field_names) {
        let hash = field_names.reduce(function(acc, curr, idx){
          acc[curr] = row[idx];
          return acc;
        }, {});
        return [row[0], hash];
      }
    );
    
    return _promise_for_stream(cassy_stream, sample_data_file)
    .then(function(){
      return cc.executeAsync(`select * from data2`, {prepare: true});
    }).then(function(results){
      expect(results.rows).to.have.length(1);
      let r = results.rows[0];
      let expected = {
        product_id: 'pid1',
        details: {
          category_list: 'cat1|cat2',
          color: 'blue',
          product_id: 'pid1'
        }
      };
      expect(r.product_id).to.eql(expected.product_id);
      expect(r.details).to.eql(expected.details);
    });
  });
  
});
