/* global require, console, process */
var redindex = require("../index.js"),
	assert = require("assert");

redindex.client.select(1, function() {
	var messages = redindex('messages', {
		indexes: {
			totime: function (room, emit) {
				emit(room.to, room.time);
			}
		}
	});
	
	messages.put({id: 'msg01', to: 'room1', time: 10});
	messages.put({id: 'msg02', to: 'room2', time: 12});
	messages.put({id: 'msg03', to: 'room1', time: 13});
	messages.put({id: 'msg04', to: 'room1', time: 15});
	messages.put({id: 'msg05', to: 'room3', time: 15});
	messages.put({id: 'msg06', to: 'room1', time: 16});
	messages.put({id: 'msg07', to: 'room3', time: 17});
	messages.put({id: 'msg08', to: 'room1', time: 18});
	messages.put({id: 'msg09', to: 'room2', time: 21});
	messages.put({id: 'msg10', to: 'room1', time: 22});
	
	messages.get({by: 'totime', str: 'room1', num: {gt: 10, lte: 18}, full: true, rev: true, limit: 3, offset: 1}, function(err, res) {
		if(err) throw err;
		assert.deepEqual(res, [{id:'msg06',to:'room1',time:16},{id:'msg04',to:'room1',time:15},{id:'msg03',to:'room1',time:13}]);
		console.log("All tests passed");
	});
	process.exit();
});