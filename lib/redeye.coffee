# Red Eye
# =======
# 
# Fast parallel workers
# ---------------------
#
# Red eye workers handle a single job until completion. The runner defined
# by the prefix of the job contains the code to perform the computation.
# The runner uses the context of the worker, and has access to three important
# methods:
# 
# * `@get(key)`: returns named key from the database
# * `@emit(key, value)`: stores value for named key to the database
# * `@for_reals()`: stop and make sure all dependencies have been met
# 
# The first time a key is requested with `@get`, the true value of that
# key is not returned. Only after calling `@for_reals()` are those variables
# substituted with their actual values. In order to minimize total latency,
# you should use as few calls to `@for_reals` as possible, but remember that
# the values returned by `@get` aren't useful until then.
# 
# The runner function is called with the arguments of the job. It can either
# use `@emit` to indicate its result(s), or it can simply return a single
# result from the function, but not both.

WorkQueue = require './work_queue'
exports.queue = (options) -> new WorkQueue(options ? {})
