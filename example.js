var sys = require('sys');
var moo = require(__dirname + '/node-moo').moo;

var storage = moo('localhost', 11211, 3600, 'moo');
// single
sys.puts("single ------------");
storage.connect(function(client){
  return client.has('/hoge', function (exists){
    if(exists){
      return client.get('/hoge', function (value){
        sys.puts(value);
        return 'hello world';
      });
    }
    return client.set('/hoge', 'example');
  });
});

// nest
sys.puts("nest ------------");
var a = storage.connect(function(client){
  // call #1
  return client.get('/foo', function (value){
    value = Number(value);
    sys.puts("1. value => " + value); // 1. value => 0
    return value + 1;
  }).then(function (){
    // call #2
    return client.get('/foo', function(value){
      value = Number(value);
      sys.puts("2. value => " + value); // 2. value => 1
      return value + 2;
    });
  }).then(function (){
    // call #3
    return client.get('/foo', function(value){
      value = Number(value);
      sys.puts("3. value => " + value); // 3. value => 3
      return value - 2;
    }).then(function (){
      // call #4
      return client.get('/foo', function(value){
        value = Number(value);
        sys.puts("4. value => " + value); // 4. value => 1;
        return value + 1;
      });
    });
  }).then(function (){
    // call #5
    return client.get('/foo', function(value){
      value = Number(value);
      sys.puts("5. value => " + value); // 5. value => 2
      return value + 1;
    });
    
  });
});

storage.list('/bar', function (list){
  list.push(1);
  list.push(2);
  list.push(3);
  for(var i = 1; i <= 5; ++i){
    list.push(i * 10);
  }
  list.each(20, 10, function (_, value){
    sys.puts(value);
  });
  list.count(function(_, count){
    sys.puts('total => ' + count);
  });
});
