"use strict"

let async = require("async");
let influx = require("influx");

let DataBuffer = require("./utils/dataBuffer");

class WritePump {
	constructor(config) {
		this.buffer = new DataBuffer(config.writeMaxPoints)
		this.name = config.name;
		this.config = config;
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

		let writeLimit = this.config.writeMaxPoints || 1000;
		let writeInterval = this.config.writeInterval || 5000;

		console.log(this.name, ": starting writepump [ writeLimit: ", writeLimit, ", writeInterval:", writeInterval, "].")

		this.buffer.on('data', data => {
			this.output.writePoints(data, {database : this.config.database})
			.then(() => {
				
			})
			.catch(err => {
				if (err.toString().search("database not found") != -1) {
					console.log('Create Database ' + self.config.database);
					this.output.createDatabase(self.config.database);
				} else {
					console.log("Could not write to Database");
				}
			})
		})

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
