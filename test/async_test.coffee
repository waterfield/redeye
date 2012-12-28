test '@async', ->

  worker 'test', ->
    @async (callback) ->
      setTimeout (-> callback null, 216), 100

  setup -> request 'test'
  want 216
