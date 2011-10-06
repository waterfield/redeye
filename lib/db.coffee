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

# Toggle this to enable LOTS of debug output.
redis.debug_mode = false

# Constructs a new client that listens for errors. The optional
# `db_index` chooses which database to `SELECT` (defaults to 0).
make_client = (db_index) ->
  db = redis.createClient port, host
  db.on 'error', (err) -> throw err
  db.select db_index if db_index?
  db

# Export the client-maker. 
module.exports = make_client
