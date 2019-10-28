const cacache = require('cacache');
const ssri = require('ssri');
const stringify = require('fast-safe-stringify')
const fs = require('fs');
const WorkerNodes = require('worker-nodes');
const Path = require('path');

const queues = {};
const algorithms = ['sha512'];


exports.worker = function(opts) {
	const worker = new WorkerNodes(Path.join(opts.dirname, opts.filename || 'worker.js'), {
		taskTimeout: opts.timeout || 60 * 1000,
		minWorkers: opts.min || 1,
		maxWorkers: opts.max || 2
	});
	process.on('exit', function() {
		worker.terminate();
	});
	return function(inputs, output, opts) {
		if (inputs.length == 0) return Promise.resolve();
		return exports.run(inputs, output, opts, function(input, data, opts) {
			return worker.call(input, data, output, opts);
		});
	};
};

exports.run = function(inputs, output, opts, transform) {
	const outputStream = fs.createWriteStream(output);
	const memo = {};
	return Promise.all(inputs.map(function(input) {
		return fs.promises.readFile(input).then(function(buf) {
			const data = buf.toString();
			return cacheable(opts.cache || {}, function() {
				if (!memo.hash) memo.hash = stringify(opts);
				return ssri.create({
					algorithms: algorithms
				}).update(memo.hash).update(data).digest().toString();
			}, function() {
				return Promise.resolve().then(function() {
					return transform(input, data, opts);
				});
			});
		});
	})).then(function(list) {
		list.forEach((item) => {
			outputStream.write(item.data);
		});
		var p = new Promise(function(resolve, reject) {
			outputStream.on('finish', resolve);
			outputStream.on("error", reject);
		});
		outputStream.end();
		return p;
	});
};

function cacheable(opts, checksum, fn) {
	if (!opts.dir) return fn();
	var key = checksum();
	return concurrent(key, opts, function() {
		return cacache.get(opts.dir, key).catch(function() {
			// not found
			return fn().then(function(result) {
				return cacache.put(opts.dir, key, result.data, {
					algorithms: algorithms
				}).then((integrity) => {
					result.integrity = integrity;
					return result;
				});
			});
		});
	});
}

function concurrent(key, opts, fn) {
	let q = queues[key];
	if (!q) {
		q = {
			waiters: 0,
			p: fn()
		};
	}
	q.waiters++;
	q.p = q.p.then(function(data) {
		q.waiters--;
		if (q.waiters == 0) {
			setTimeout(function() {
				if (q.waiters == 0 && !q.dead) {
					q.dead = true;
					delete queues[key];
				}
			}, opts.timeout || 60000);
		}
		return data;
	});
	return q.p;
}

