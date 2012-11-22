Manager = require './lib/manager'

m = new Manager

m.worker 'foo', ->
  @all ->
    for i in [0..2]
      @bar()

m.worker 'bar', -> 'bar'

m.run()
