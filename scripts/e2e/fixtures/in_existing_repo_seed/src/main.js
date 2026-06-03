const { greet } = require("./util")

function main() {
  const name = process.env.USER_NAME || "world"
  console.log(greet(name))
}

if (require.main === module) {
  main()
}

module.exports = { main }
