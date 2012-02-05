redeye_suite = require './support/redeye_suite'

big_num = 1000

module.exports = redeye_suite

  # Tests that a pathological many-step case is still fast
  # (usually clocks in at < 1ms per iteration. not bad!)
  'test speed':
  
    workers:
      # 'n' just counts down
      n: (i) ->
        console.log i
        i = parseInt i
        if i == 1 then 1 else @get_now('n', i-1) + 1

    # Request a big job. Record when the request is made.
    setup: ->
      @start_time = new Date().getTime()
      @request 'n', big_num

    # Assert that the job didn't take a really long time.
    expect: ->
      dt = new Date().getTime() - @start_time
      @db.get "n:#{big_num}", (err, str) =>
        @assert.equal str, ''+big_num
        @assert.equal true,  (dt < 2000)
        @finish()
