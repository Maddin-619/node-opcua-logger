const EE = require("events").EventEmitter

class DataBuffer extends EE {
    constructor(count, writeInterval = 2000) {
        super()
        this.count = count
        this.q = []
        this.reachedLimit = false
        setInterval(() => {
            if (this.reachedLimit) {
                this.reachedLimit = false
            } else if (this.q.length > 0)  {
                this.emit('data', this.q)
                this.q = []
            }
        }, writeInterval)
    }

    insert(data) {
        this.q.push(data)
        if (this.q.length == this.count) {
            this.reachedLimit = true
            this.emit('data', this.q)
            this.q = []
        }
    }
    
    get() {
        return this.q
    }
}
module.exports = DataBuffer
