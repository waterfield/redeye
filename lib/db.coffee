# Database wrapper for Redis
# ==========================
# 
# You can make a new connection like this:
# 
#     db = require('./db')()

# Require redis itself
redis = require 'redis'

# Host and port configuration
host = '127.0.0.1'
port = 6379

# Constructs a new client that listens for errors
make_client = ->
  db = redis.createClient port, host
  db.on 'error', (err) -> throw err
  db

# Export the client-maker. 
module.exports = make_client
