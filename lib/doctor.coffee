class Doctor

  constructor: (@deps, @state, @seed) ->

  diagnose: ->
    @clear()
    @invert_deps()
    @scan @seed

  report: ->
    @report_loose_ends()
    @report_cycles()
  
  report_cycles: ->
    #return unless @cycles.count
    console.log "Cycles:"
    for cycle in @cycles
      console.log "  #{cycle.join ' -> '}"
  
  report_loose_ends: ->
    #return unless @loose_ends.count
    console.log "Loose ends: #{@loose_ends.join(', ')}"

  clear: ->
    @inv = {}
    @cycles = []
    @loose_ends = []
    @stack = []

  scan: (node) ->
    if node in @stack
      @cycles.push @stack[0..-1]
      return
    @stack.push node
    nexts = @inv[node] ? []
    for next in nexts
      @scan next
    unless nexts.length
      unless @state[node] == 'done'
        @loose_ends.push node
    @stack.pop()

  invert_deps: ->
    for source, targets of @deps
      for target in targets
        sources = (@inv[target] ?= [])
        sources.push source

module.exports = Doctor