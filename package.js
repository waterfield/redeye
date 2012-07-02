require('coffee-script');
var config = require('./lib/config');
module.exports = {
  consts: require('./lib/consts'),
  db: require('./lib/db'),
  configure: function(settings) { config.provide(settings); }
}