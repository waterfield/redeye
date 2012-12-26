# make sure each key takes < 10ms
ms_per_key = 10

test 'speed', ->

  big_num = 1000

  worker 'n', (i) -> if i == 1 then 1 else @n(i-1) + 1

  setup ->
    $.start = new Date().getTime()
    request 'n', big_num

  want big_num
  expect ->
    dt = new Date().getTime() - $.start
    assert.that dt < (big_num * ms_per_key)
