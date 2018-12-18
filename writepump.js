"use strict"

let influx = require("influx");

let DataBuffer = require("./utils/dataBuffer");

class WritePump {
	constructor(config) {
		this.buffer = new DataBuffer(config.writeMaxPoints || 10000 , config.writeInterval || 2000)
		this.name = config.name;
		this.config = config;
		this.shutdownOnce = false;
		this.output = new influx.InfluxDB({
			host :           config.host,
			port :           config.port, // optional, default 8086
			protocol :       config.protocol, // optional, default 'http'
			username :       config.username,
			password :       config.password,
			options  :       {timeout: config.failoverTimeout}
		});
	}

	Run() {

		console.log(this.name, ": starting writepump [ writeLimit: ", this.config.writeMaxPoints, ", writeInterval:", this.config.writeInterval, "].")

		this.buffer.startIntervalRead()

		this.buffer.on('data', data => {
			this.output.writePoints(data, {database : this.config.database})
			.catch(err => {
				if (err.toString().search("database not found") != -1) {
					console.log('Create Database ' + this.config.database);
					this.output.createDatabase(this.config.database);
				} else {
					console.log(err);
				}
			})
		})

	}

	Stop() {
		if (!this.shutdownOnce) {
			console.log('Shut down Writepump', this.name)
			this.buffer.removeAllListeners()
			this.buffer.stopIntervalRead()
			this.shutdownOnce = true
		}
	}

	AddPointsToBuffer(points){
		// points must be transformed to only the required info, otherwise 
		// buffer overhead would be to large. Immediatly transform to a format 
		// that is easy for influx later on.
		points.forEach( p => {
				let entry = {
					measurement: p.measurement.name, 
					fields: {value: p.value},
					timestamp: new Date(p.timestamp),
					tags: p.measurement.tags
				};
				// opc status should also be included in tags.
				entry.tags.opcstatus = p.opcstatus;
				this.buffer.insert(entry)
			}
		);
	}
}

module.exports = WritePump;
