_ = require 'underscore'

# The Doctor scans the dependencies and state of requests to determine
# why no progress is being made. It looks for cyclic dependencies and
# plain unsatisfied dependencies, and can report them.
class Doctor

  # Create a new doctor based on the given dependencies
  # (in `{dependency: [dependent, ...]}` form), state of jobs,
  # and the seed job.
  constructor: (@deps, @state, @seeds) ->

  # Scan the information to determine what's wrong
  diagnose: ->
    @clear()
    @invert_deps()
    @scan seed for seed in @seeds
    @uniqify_cycles()

  # Print a report about what's broken
  report: ->
    # @report_deps()
    # @report_state()
    @report_loose_ends()
    @report_cycles()
  
  # Print out the dependencies
  report_deps: ->
    for key, values of @inv
      console.log "#{key} -> #{values.join ', '}"
  
  # Report on the state of affairs
  report_state: ->
    for key, state of @state
      console.log "#{key} :: #{state}"
  
  # Print out a list of cyclic dependencies. For instance,
  # 
  #     A -> B -> C
  # 
  # means that C cycles back around to A.
  report_cycles: ->
    return unless @cycles.length
    console.log "Cycles:"
    for cycle in @cycles
      console.log "  #{cycle.join ' -> '}"
  
  # Report on loose ends, that is, unsatisfied dependencies that
  # aren't part of cycles.
  report_loose_ends: ->
    return unless @has_loose_ends
    console.log "Loose ends:"
    for node, stack of @loose_ends
      console.log "  #{node}: #{stack.join ','}"
  
  # Remove versions of cycles that are duplicates
  uniqify_cycles: ->
    map = {}
    map[cycle.sort().join()] ?= cycle for cycle in @cycles
    @cycles = _.values map
  
  is_stuck: ->
    @has_loose_ends || @cycles.length

  # Reset the doctor's diagnosis for another run.
  clear: ->
    @inv = {}
    @cycles = []
    @loose_ends = []
    @stack = []
    @has_loose_ends = false
  
  # Recursive scanning method. Simultaneously determines
  # cycles and graph leaves.
  scan: (node) ->
    idx = @stack.indexOf node
    if idx >= 0
      @cycles.push @stack[idx..-1]
      return
    @stack.push node
    nexts = @inv[node] ? []
    for next in nexts
      @scan next
    unless nexts.length
      unless @state[node] == 'done'
        @add_loose_end node, @stack[0..-1]
    @stack.pop()
  
  add_loose_end: (node, stack) ->
    return if @loose_ends[node]
    @has_loose_ends = true
    @loose_ends[node] = stack

  # Convert the input form of dependencies to a more straightforward version.
  # For instance, it converts
  # 
  #     {'A': ['B', 'C'], 'B': ['C']}
  # 
  # to
  # 
  #     {'B': ['A'], 'C': ['A', 'B']}
  invert_deps: ->
    for source, targets of @deps
      for target in targets
        sources = (@inv[target] ?= [])
        sources.push source
  
  # Find a map of each stuck key, and the key(s) it depends on.
  cycle_dependencies: ->
    deps = {}
    for cycle in @cycles
      (deps[cycle[cycle.length-1]] ||= {})[cycle[0]] = true
      for i in [1...cycle.length]
        (deps[cycle[i-1]] ||= {})[cycle[i]] = true
    for key, hash of deps
      deps[key] = _.keys hash
    deps
  
  # Return whether the doctor is stuck due to cycles, and not loose ends.
  recoverable: ->
    !@has_loose_ends && (@cycles.length > 0)

module.exports = Doctor