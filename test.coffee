Manager = require './lib/manager'

m = new Manager

m.worker 'foo', -> @bar() + @baz()
m.worker 'bar', -> 'bar'
m.worker 'baz', -> 'baz'

m.run()
