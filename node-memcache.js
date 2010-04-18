var net = require('net'),
    sys = require('sys'),
    md5 = require('md5');
    
var crlf = "\r\n";
var crlf_len = crlf.length;

var error_replies = ['ERROR', 'NOT_FOUND', 'CLIENT_ERROR', 'SERVER_ERROR'];

var reply_indicators = {
    'get' : ['VALUE', 'END'],
    'set' : ['STORED', 'NOT_STORED', 'EXISTS'],
    'stats' : ['STATS'],
    'delete' : ['DELETED'],
    'version' : ['VERSION']
};

var Client = exports.Client = function(port, host) {
    this.port = port || 11211;
    this.host = host || 'localhost';
    this.buffer = '';
    this.conn = null;
    this.sends = 0;
    this.receives = 0;
    this.callbacks = [];
    this.replies = 0;
    this.handles = [];
};

sys.inherits(Client, process.EventEmitter);

Client.prototype.connect = function (callback) {
    if (!this.conn) {
        var conn = new net.createConnection(this.port, this.host);
        conn.setTimeout(0); // try to stay connected.
        conn.setNoDelay(true);
        conn.setEncoding('utf-8');
        
        var self = this;
        conn.addListener('connect', function () {
              self.emit('connect');
              self.dispatchHandles();
        }); 
     
        conn.addListener('data', function (data) {
            self.buffer += data;
            // sys.debug(data);
            self.recieves += 1;
            self.handle_received_data();
        });
     
        conn.addListener('end', function () {
            if (self.conn && self.conn.readyState) {
                self.conn.end();
                self.conn = null;
            }
        });
     
        conn.addListener('close', function () {
            self.conn = null;
            self.emit('close');
        });
        this.conn = conn;
    }
    if(callback){
        this.addHandler(callback);
    }
};

Client.prototype.addHandler = function(callback) {
    this.handles.push(callback);
    
    if (this.conn.readyState == 'open') {
        this.dispatchHandles();
    }
};

Client.prototype.dispatchHandles = function() {
    for (var i in this.handles) {
        var handle = this.handles.shift();
        // sys.debug('dispatching handle ' + handle);
        handle();
    }
};

Client.prototype.query = function(query, type, callback) {
    var self  = this;
    var data = query + crlf;
    this.callbacks.push({ type: type, fun: callback });
    self.sends += 1;
    this.conn.write(data);
};

Client.prototype.close = function(idx) {
    if (this.conn && this.conn.readyState === 'open') {
        this.conn.end();
        this.conn = null;
    }
};

Client.prototype.get = function(key, callback) {
    return this.query('get ' + key, 'get', callback);
};

Client.prototype.set = function(key, value, lifetime, callback) {
    var exp_time  = lifetime || 0;
    var value_len = String(value).length || 0;
    var query = 'set ' + key + ' 0 ' + exp_time + ' ' + value_len + crlf + value;

    return this.query(query, 'set', callback);
};

Client.prototype.del = function(key, callback) {
    return this.query('delete ' + key, 'delete', callback);
};

Client.prototype.version = function(callback) {
    return this.query('version', 'version', callback);
};

Client.prototype.increment = function(key, value, callback) {
    value = value || 1;
    return this.query('incr ' + key + ' ' + value, 'incr', callback);
};

Client.prototype.decrement = function(key, value, callback) {
    value = value || 1;
    return this.query('decr ' + key + ' ' + value, 'decr', callback);
};

Client.prototype.handle_received_data = function () {
    var self = this;
    
    while (this.buffer.length > 0) {
        var result = this.determine_reply_handler(this.buffer);
        
        if (result == null) {
            break;
        }
        
        var result_value = result[0];
        var next_result_at = result[1];
        
        if (next_result_at > this.buffer.length) {
            break;
        }
        
        var callback = this.callbacks.shift();
        
        if (result_value === null) {
            throw 'Error';
        }
        
        this.buffer = this.buffer.substring(next_result_at);
        if (callback.fun) {
            this.replies += 1;
            callback.fun(result_value);
        }
    }
};

Client.prototype.determine_reply_handler = function (buffer) {
    var crlf_at = buffer.indexOf(crlf);
    if (crlf_at == -1) {
        return null;
    }
    
    var first_line = buffer.substr(0, crlf_at);
    if (parseInt(first_line) == first_line) {
        return this.handle_integer(buffer, crlf_at);
    }
        
    // determine errors
    for (var error_idx in error_replies) {
        var error_indicator = error_replies[error_idx];
        if (buffer.indexOf(error_indicator) == 0) {
            return this.handle_error(buffer);
        }
    }
    
    // determine normal reply handler
    for (var method in reply_indicators) {
        for (var indicator in reply_indicators[method]) {
            var current_indicator = reply_indicators[method][indicator];
            if (buffer.indexOf(current_indicator) == 0) {
                return this['handle_' + method](buffer);
            }
        }
    }
    
    return null;
};

Client.prototype.handle_get = function(buffer) {
    var next_result_at = 0;
    var result_value = '';
    var end_indicator_len = 3;
    
    if (buffer.indexOf('END') == 0) {
        return [result_value, end_indicator_len + crlf_len];
    }
    var first_line_len = buffer.indexOf(crlf) + crlf_len;
    var result_len     = buffer.substr(0, first_line_len).split(' ')[3];
    result_value       = buffer.substr(first_line_len, result_len);
    
    return [result_value, first_line_len + parseInt(result_len ) + crlf_len + end_indicator_len + crlf_len];
};

Client.prototype.handle_delete = function(buffer) {
    var result_value = 'DELETED';
    return [result_value, result_value.length + crlf_len];
};

Client.prototype.handle_set = function(buffer) {
    var result_value = 'STORED';
    return [result_value, result_value.length + crlf_len];
};

Client.prototype.handle_version = function(buffer) {
    var line_len      = buffer.indexOf(crlf);
    var indicator_len = 'VERSION '.length;
    var result_value  = buffer.substr(indicator_len, (line_len - indicator_len));
    return [result_value, line_len + crlf_len];
};

Client.prototype.handle_integer = function(buffer, line_len) {
    var result_value  = buffer.substr(0, line_len);
    return [result_value, line_len + crlf_len];
};

Client.prototype.handle_error = function(buffer) {
    line = Client.readLine(buffer);
    
    return [null, (line.length + crlf_len)];
};

Client.readLine = function(string) {
    var line_len = string.indexOf(crlf);
    return string.substr(0, line_len);
};

var Pool = exports.Pool = function(hosts, hashFunction, numberOfReplicas){
  var hashFunction = hashFunction || Hash.MD5;
  var numberOfReplicas = numberOfReplicas || 128;
  var curry = function(hir, params){
    return function (){
      return hir.apply(this, params);
    };
  };
  var pool = {};
  for(var i = 0; i < hosts.length; ++i){
    var value = hosts[i];
    var client = new Client;
    client.host = value.host;
    client.port = value.port;
    client.connect(curry(this.onConnect, [this, i]));
    pool[i] = {
      'client': client,
      'connected': false
    };
  }
  this.concisntentHash = new ConsistentHah(hashFunction, numberOfReplicas);
  this.length = hosts.length;
  this.pool = pool;
};
Pool.prototype = {
  onConnect: function(self, index){
    this.pool[index].connected = true;
    var pool = this.pool[index];
    var node = new ValueNode(index, pool.client);
    this.concisntentHash.add(node);
  },
  set: function(key, value, lifetime, callback){
    var node = this.concisntentHash.get(key);
    return node.value().set(key, value, lifetime, callback);
  },
  get: function(key, callback){
    var node = this.concisntentHash.get(key);
    return node.value().get(key, callback);
  }
};

var HashFunction = function (){};
HashFunction.prototype = {
  hash: function (str){
    throw new Error('not yet implemented');
  }
};

var Hash = {};
Hash.MD5 = function (){};
Hash.MD5.prototype = new HashFunction;
Hash.MD5.prototype.hash = function (str){
  return md5.hex(str);
};

var Circle = function (values){
  this.values = values || [];
};
Circle.prototype = {
  put: function (key, value){
    this.values[key] = value;
  },
  get: function (key){
    return this.values[key];
  },
  remove: function (key){
    delete this.values[key];
  },
  has: function (key){
    return typeof this.values[key] != 'undefined';
  },
  isEmpty: function (){
    return this.values.length < 1;
  },
  keys: function (){
    var keys = [];
    for(var key in this.values){
      if(!this.values.hasOwnProperty(key)){
        continue;
      }
      keys.push(key);
    }
    return keys;
  }
};

var Node = function (){};
Node.prototype = {
  getName: function (){
    throw new Error('not yet implemented');
  }
};
var ContainerNode = function (){};
ContainerNode.prototype = new Node;
ContainerNode.prototype.put = function (key, value){
  throw new Error('not yet implemented');
};
ContainerNode.prototype.get = function (key){
  throw new Error('not yet implemented');
};
ContainerNode.prototype.has = function (key){
  throw new Error('not yet implemented');
};
ContainerNode.prototype.keys = function (){
  throw new Error('not yet implemented');
};

var TreeMap = Circle.prototype.constructor;
TreeMap.prototype = new Circle;
TreeMap.prototype.firstKey = function (){
  var keys = this.keys();
  keys.sort();
  return keys[0];
};
TreeMap.prototype.tailMap = function (key){
  var self = this;
  var keys = self.keys();
  keys.sort();

  var results = []:
  keys.forEach(function(value){
    if(key <= value){
      results[value] = self[value];
    }
  });
  return new TreeMap(results);
};

var ConsistentHah = function(hashFunction, numberOfReplicas){
  this.hashFunction = hashFunction;
  this.numberOfReplicas = numberOfReplicas;
  this.circle = new TreeMap;
  this.nodes = [];
};
ConsistentHah.prototype = {
  getNodes: function (){
    return this.nodes;
  },
  add: function(node){
    for(var i = 0; i < this.numberOfReplicas; ++i){
      var nodeKey = this.hashFunction.hash(node.getName() + i);
      this.circle.put(nodeKey, node);
    }
    this.nodes.push(node);
  },
  get: function(key){
    if(this.circle.isEmpty()){
      return null;
    }
    var hash = this.hashFunction.hash(key);
    if(!this.circle.has(hash)){
      var tailMap = this.circle.tailMap(hash);
      if(tailMap.isEmpty()){
        hash = this.circle.firstKey();
      } else {
        hash = tailMap.firstKey();
      }
    }
    return this.circle.get(hash);
  },
  remove: function(node){
    for(var i = 0; i < this.numberOfReplicas; ++i){
      var nodeKey = this.hashFunction.hash(node.getName() + i);
      this.circle.remove(nodeKey, node);
    }
  }
};

var ValueNode = function(name, value){
  this.name = name;
  this.value = value;
};
ValueNode.prototype = new Node;
ValueNode.prototype.value = function (){
  return this.value;
};
ValueNode.prototype.getName = function (){
  return this.name;
};

var ConsistentHashNode = function(consistentHah){
  this.consistentHah = consistentHah;
  this.keys = [];
};
ConsistentHashNode.prototype = new ContainerNode;
ConsistentHashNode.prototype.put = function (key, value){
  this.consistentHah.get(key).put(key, value);
  this.keys.push(key);
};
ConsistentHashNode.prototype.get = function (key){
  return this.consistentHah.get(key).get(key);
};
ConsistentHashNode.prototype.has = function (key){
  return this.consistentHah.get(key).has(key);
};
ConsistentHashNode.prototype.keys = function (){
  return this.keys;
};
ConsistentHashNode.prototype.getName = function (){
  return 'ConsistentHashNode';
};

var IdentNode = function (name){
  this.name = name;
  this.values = [];
};
IdentNode.prototype = new ContainerNode;
IdentNode.prototype.put = function(key, value){
  this.values[key] = value;
};
IdentNode.prototype.get = function (key){
  return this.values[key];
};
IdentNode.prototype.has = function (key){
  return typeof this.values[key] != 'undefined';
};
IdentNode.prototype.keys = function (){
  var keys = [];
  for(var key in this.values){
    if(!this.values.hasOwnProperty(key)){
      continue;
    }
    keys.push(key);
  }
  return keys;
};
IdentNode.prototype.getNames = function (){
  return this.name;
};

