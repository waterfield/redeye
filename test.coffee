Manager = require './lib/manager'

m = new Manager

m.worker 'foo', -> 'repeating ' + @bar()
m.worker 'bar', -> 'the answer!'

m.run()
