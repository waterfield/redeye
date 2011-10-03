class Doctor

  constructor: (@deps, @state, @seed) ->

  diagnose: ->
    @clear()
    @invert_deps()
    @find_cycles()
    @find_loose_ends()

  report: ->
    @report_loose_ends()
    @report_cycles()
  
  report_cycles: ->
    return unless @cycles.count
    console.log "Cycles:"
    for cycle in @cycles
      console.log "  #{cycle.join ' -> '}"
  
  report_loose_ends: ->
    return unless @loose_ends.count
    console.log "Loose ends: #{@loose_ends.join(', ')}"

  clear: ->
    @inv = {}
    @cycles = []
    @loose_ends = []

  find_cycles: ->
    @stack = []
    @scan_cycles @seed
  
  scan_cycles: (node) ->
    if node in @stack
      @cycles.push @stack
      return
    @stack.push node
    for next in @inv[node] ? []
      scan_cycles next
    @stack.pop()

  find_loose_ends: ->
    scan_loose_ends @seed
  
  scan_loose_ends: (node) ->
    return if @state[node] == 'done'
    if nexts = @inv[node]
      scan_loose_ends node for node in nexts
    else if !(node in @loose_ends)
      @loose_ends.push node

  invert_deps: ->
    for source, targets of @deps
      for target in targets
        sources = (@inv[target] ?= [])
        sources.push source
