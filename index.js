/* global require, console */

var redis = require('redis'),
	client = redis.createClient(),
	noop = function() {};

var redindex = function(type, opt) {
	opt.indexes = opt.indexes || {};
	
	/*
		Calculates the index str and num values, and returns something like:
		{
			key1: [key1, num1, id, num2, id, num3, id]
			key2: [key2, num4, id, num5, id]
		}
		
		Each value in the hashmap is a list of arguments for redis.
	*/
	function indexes(obj) {
		var ino = {}, i;
		
		function push(str, num) {
			var key = type + '/' + i + (str? ':' + str: '');
			ino[key] = (ino[key] || [key]).concat(num || 0, obj.id);
		}
		
		for(i in opt.indexes) {
			opt.indexes[i](obj, push);
		}
		
		return ino;
	}
	
	return {
		put: function(obj, cb) {
			cb = cb || noop;
			if(!obj.id) return cb(Error("ERR_PUT_BAD_OBJ" + JSON.stringify(obj)));
			
			client.getset(type + ':' + obj.id, JSON.stringify(obj), function(err, old) {
				var multi, ino, oldino, key;
				
				if(err) return cb(Error("ERR_PUT_GETSET" + err.message));
				ino = indexes(obj);
				
				
				multi = client.multi();
				for(key in ino) multi.zadd(ino[key]);
				
				if(old) for(key in indexes(JSON.parse(old))) {
					if(typeof ino[key] === 'undefined') multi.zrem(key, obj.id);
				}
				
				multi.exec(cb);
			});
		},
		
		get: function(q, cb) {
			var key = type + '/' + q.by,
				lo = '-inf', hi = '+inf';
			cb = cb || noop;

			function byId(ids) {
				client.mget(ids.map(function(id) { return type + ':' + id; }), function(err, data) {
					if(err) return cb(Error("ERR_GET_BY_ID " + err.message));
					cb(null, data.map(JSON.parse));
				});
			}
			
			function results(err, data){
				if(err) return cb(Error("ERR_GET_BY_QUERY " + err.message + ' ' + JSON.stringify(q)));
				if(!data || !data.length || !q.full) cb(null, data);
				byId(data);
			}
			
			if(typeof q !== 'object') {
				byId([q]);
			} else if(q.by && (q.str || q.num)) {
				if(q.str) key += ':' + q.str;
				
				q.offset = q.offset || 0;
				q.limit  = (!q.limit || q.limit > 1024)? 1024: q.limit;
				
				if(typeof q.num === 'number') {
					lo = hi = q.num;
				} else if(typeof q.num === 'object') {
					if(q.num.lt)  hi = '(' + q.num.lt;
					if(q.num.lte) hi = q.num.lte;
					if(q.num.gt)  lo = '(' + q.num.gt;
					if(q.num.gte) lo = q.num.gte;
					if(q.num.eq)  lo = hi = q.num.eq;
				}
				
				if(q.rev) {
					client.zrevrangebyscore([key, hi, lo, 'LIMIT', q.offset, q.limit], results);
				} else {
					client.zrangebyscore([key, lo, hi, 'LIMIT', q.offset, q.limit], results);
				}

			} else {
				return cb(Error("ERR_GET_BAD_QUERY " + JSON.stringify(q)));
			}
		},
	
		del: function(id, cb) {
			cb = cb || noop;
			var multi = client.multi(),
				key = type + ':' + id,
				obj;
			
			client.get(key, function(err, data) {
				if(err) return cb("ERR_DEL_GET " + err.message);
				if(!data) return cb();
				
				multi.del(key);
				for(key in indexes(JSON.parse(obj))) {
					multi.zrem(key, obj.id);
				}
				multi.exec(cb);
			});
		}
	};
};

redindex.client = client;
module.exports = redindex;