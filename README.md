# CassandraWriteStream

## Example usage
```javascript
//simply create the CassandraWriteStream...
let cassy_stream = new CassandraWriteStream(
  {contactPoints: ['127.0.0.1', '127.0.0.2'], keyspace: test_keyspace_name},
  `INSERT INTO data (product_id, field, value) 
    VALUES (?, ?, ?) USING TTL ${1000}`,
  {prepare: true, isIdempotent: true}
);

//and then pipe any Readable to it!
let s = new Readable();
s.pipe(cassy_stream)
.on('finish', finish_handler)
.on('error', error_handler);
```
