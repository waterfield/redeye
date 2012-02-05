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
        if i == '1'
          1
        else
          n1 = @get('n', parseInt(i) - 1)
          console.log 'n1 is', n1, typeof(n1) # XXX
          kk = n1 + 1
          console.log 'kk is', kk # XXX
          kk

    # Request a big job. Record when the request is made.
    setup: ->
      @start_time = new Date().getTime()
      @request 'n', big_num

    # Assert that the job didn't take a really long time.
    expect: ->
      dt = new Date().getTime() - @start_time
      @get @requested, (val) =>
        console.log 'done'
        @assert.equal val, big_num
        @assert.equal true, (dt < 2000)
        @finish()
