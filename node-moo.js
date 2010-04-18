//
// memcache object store
// @nowelium
//

var sys = require('sys');
var memcache = require(__dirname + '/node-memcache');

if(typeof JSON.encode == 'undefined'){
  JSON.encode = JSON.stringify;
}
if(typeof JSON.decode == 'undefined'){
  JSON.decode = JSON.parse;
}

var Moo = function (host, port, lifetime, namespace) {
  var pathSeparator = this.pathSeparator;
  var prefix = namespace;
  
  var genKey = function (key){
    return prefix + ':' + key;
  };

  var store = function (client, path, value, time){
    if(typeof value == 'undefined'){
      return ;
    }
    if(typeof value == 'number'){
      return client.set(path, String(value), time);
    }
    if(typeof value == 'string'){
      return client.set(path, '"' + value + '"', time);
    }
    for(var property in value){
      var p = path;
      if(/\/$/.test(p)){
        p = path + property;
      } else {
        p = path + pathSeparator + property;
      }
      store(client, p, value[property], time);
    }
    return ;
  };
  
  this.callback = function (callback){
    var wrapper = new Moo.Wrapper({
      client: new Moo.Client(host, port),
      genKey: genKey,
      store: store,
      lifetime: lifetime
    });
    return callback(wrapper);
  };
};
Moo.prototype = {
  pathSeparator: '/',
  connect: function(callback){
    return this.callback(function (wrapper){
      return wrapper.connect().then(function (){
        return callback.apply(wrapper, [wrapper]);
      }).then(function (){
        return wrapper.disconnect();
      });
    });
  },
  list: function(key, callback){
    return this.callback(function(wrapper){
      var chain = wrapper.connect();
      var list = new Moo.List(chain, key, wrapper);
      callback.apply(list, [list]);
      
      return list.chain.then(function (){
        return wrapper.disconnect();
      });
    });
  }
};
Moo.Wrapper = function (options){
  var client = options.client;
  var genKey = options.genKey;
  var store = options.store;
  var lifetime = options.lifetime;
  
  var self = this;
  this.connect = function(){
    return client.connect();
  };
  this.disconnect = function(){
    return client.disconnect();
  };
  this.has = function(key, callback){
    return client.has(genKey(key)).then(function (){
      return callback.apply(self, [true]);
    }).not(function (){
      return callback.apply(self, [false]);
    });
  };
  this.get = function(key, callback){
    return client.get(genKey(key)).then(function (value){
      var params = [JSON.decode(value || '""')];
      var result = callback.apply(self, params);
      return store(client, genKey(key), result, lifetime);
    });
  };
  this.set = function(key, value, expire){
    if(typeof expire == 'undefined'){
      expire = lifetime;
    }
    return store(client, genKey(key), value, expire);
  };
  this.remove = function(key, delay){
    return client.remove(key, delay);
  };
  this.lock = function(key, timeout, callback){
    return client.lock(key, timeout).then(function (){
      return callback.apply(self);
    });
  };
  this.toString = function (){
    return '[object Moo.Wrapper]';
  };
};

Moo.Client = function(host, port){
  var self = this;
  var client = new memcache.Client();
  client.host = host;
  client.port = port;
  
  this.connected = false;
  this.connect = function (){
    return new Moo.Chain(function(chain){
      return client.connect(function (){
        self.connected = true;
        return chain.success();
      }, function (e){
        throw new Error(e);
      });
    });
  };
  this.disconnect = function (){
    return new Moo.Chain(function(chain){
      self.connected = false;
      return client.close();
    });
  };
  this.get = function(key){
    return new Moo.Chain(function(chain){
      return client.get(key, function (value){
        return chain.success(value);
      }, function (e){
        return chain.success(null);
      });
    });
  };
  this.set = function(key, value, lifetime){
    return new Moo.Chain(function(chain){
      return client.set(key, value, lifetime, function (){
        return chain.success();
      }, function (e){
        return chain.failure(e);
      });
    });
  };
  this.has = function(key){
    return new Moo.Chain(function(chain){
      return client.get(key, function(value){
        return chain.success(true);
      }, function(e){
        return chain.success(false);
      });
    });
  };
  this.remove = function(key, delay){
    return new Moo.Chain(function(chain){
      return client.del(key, delay || 0, function (){
        return chain.success();
      }, function (e){
        return chain.failure(e);
      });
    });
  };
  this.lock = function(key, timeout){
    return new Moo.Chain(function (chain){
      return client.get(key, function(value){
        if('lock' == value){
          return setTimeout(function(){
            chain.success();
          }, timeout + 1);
        }
        return client.set(key, 'lock', timeout, function(value){
          return chain.success();
        }, function(ex){
          return chain.failure(ex);
        });
      });
    });
  };
  this.toString = function (){
    return '[object Moo.Client {' + (this.connected ? 'connected' : 'disconnect') + '}]';
  };
};

Moo.Chain = function (lambda){
  this.__next__ = null;
  this.__callback__ = {
    'then': function(value){
      return value;
    },
    'not': function (value){
      return value;
    }
  };

  if(lambda){
    lambda(this);
  }
};
Moo.Chain.prototype = {
  then: function (callback){
    return this.push('then', callback);
  },
  not: function (callback){
    return this.push('not', callback);
  },
  push: function (target, callback){
    this.__next__ = new Moo.Chain;
    this.__next__.__callback__[target] = callback;
    return this.__next__;
  },
  success: function (value){
    return this.call('then', value);
  },
  failure: function (value){
    return this.call('not', value);
  },
  call: function (target, value){
    try {
      var result = this.__callback__[target].apply(this, [value]);
    } catch(e) {
      target = 'not';
    }
    if(result instanceof Moo.Chain){
      result.__next__ = this.__next__;
    } else {
      if(this.__next__){
        this.__next__.call(target, value);
      }
    }
    return this;
  },
  toString: function(){
    return '[object Moo.Chain {' + this.__callback__['then'] + '},{' + this.__callback__['not'] + '}]';
  }
};

Moo.List = function(chain, key, client){
  var self = this;
  var genIncrementKey = function (){
    return key + '.increment';
  };
  var genIncrementLockKey = function (){
    return genIncrementKey() + '.lock';
  };
  var genValueKey = function (index){
    return key + '/' + index;
  };
  
  self.chain = chain;
  self.count = function (callback){
    self.chain = self.chain.then(function (){
      return client.get(genIncrementKey(), function (value){
        if(value == null){
          value = 0;
        }
        callback.apply(self, [self, value]);
      });
    });
    return self;
  };
  
  self.push = function (value){
    self.chain = self.chain.then(function (){
      return client.lock(genIncrementLockKey(), 50, function (){
        var index = -1;
        var incrementalKey = genIncrementKey();
        return client.get(incrementalKey, function (incrementValue){
          index = Number(incrementValue);
          index = index + 1;
          
          client.set(incrementalKey, index, 0);
          client.set(genValueKey(index), value, 0);
        });
      });
    });
    return self;
  };
  self.each = function(offset, limit, callback){
    self.chain = self.chain.then(function (){
      return client.get(genIncrementKey(), function (incrementValue){
        var index = Number(incrementValue);
        for(var i = offset, j = 0; i < index && j < limit; ++i, ++j){
          client.get(genValueKey(i), function (value){
            callback.apply(self, [self, value]);
          });
        }
      });
    });
    return self;
  };
  self.toString = function (){
    return '[object Moo.List]';
  };
};

Moo.getStorage = function (host, port, lifetime, namespace){
  return new Moo(host, port, lifetime, namespace || '');
};

exports.moo = Moo.getStorage;
exports.Moo = Moo;
