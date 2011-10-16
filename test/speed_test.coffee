redeye_suite = require './support/redeye_suite'

start_time = null

module.exports = redeye_suite

  # Tests that a pathological 1000-step case is still fast
  'test result and audit log':
  
    workers:
      # 'n' just counts down
      n: (i) ->
        i = parseInt i
        if i == 1 then 1 else @get_now('n', i-1) + 1

    # Request a 1000-step job. Record when the request is made.
    setup: ->
      start_time = new Date().getTime()
      @request 'n', 1000

    # Assert that the job didn't take a really long time.
    expect: ->
      dt = new Date().getTime() - start_time
      @db.get 'n:1000', (err, str) =>
        @assert.equal str, '1000'
        @assert.equal true,  (dt < 2000)
        @finish()
