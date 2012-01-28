_ = require 'underscore'
Doctor = require '../lib/doctor'

module.exports = 

  'non-repeated cycle dependencies': (exit, assert) ->
    doc = new Doctor
    doc.cycles = [['a','b','c'],['b','c','a']]
    assert.eql doc.cycle_dependencies(),
      a: ['b']
      b: ['c']
      c: ['a']
  
  'multiple cycles': (exit, assert) ->
    doc = new Doctor
    doc.cycles = [['a','b','c'],['a','c','e']]
    assert.eql doc.cycle_dependencies(),
      a: ['b', 'c']
      b: ['c']
      c: ['a', 'e']
      e: ['a']
