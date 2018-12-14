const EE = require("events").EventEmitter

class DataBuffer extends EE {
    constructor(count) {
        super();
        this.count = count;
        this.q = [];
    }

    insert(data) {
        this.q.push(data)
        if (this.q.length == this.count) {
            this.emit('data', this.q);
            this.q = [];
        }
    }
    
    get() {
        return this.q
    }
}
module.exports = DataBuffer

/*
const test = new DataBuffer(2);

test.on('data', data => {
    console.log(data);
})

test.insert("123")
console.log('get', test.get())
test.insert("123")
test.insert("123")
test.insert("123")
*/
