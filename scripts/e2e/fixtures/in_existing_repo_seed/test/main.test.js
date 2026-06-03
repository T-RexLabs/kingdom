const assert = require("assert")
const { greet } = require("../src/util")

assert.equal(greet("dev"), "hello dev")
console.log("ok")
