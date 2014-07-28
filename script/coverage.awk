#! /usr/bin/awk -f

BEGIN {
  RS = "\n"
  FS = ","
  lines = 0
  hits = 0
}

/^DA.*/ {
  lines++
  if($2 > 0) {
    hits++
  }
}

END {
  print "Total lines: "lines;
  print "Total hits: "hits;
  coverage = round((hits/lines)*100) #/# For sublime's stupid Awk plugin
  print "Coverage: "coverage"%"
}

# Found online
function round(x, ival, aval, fraction)
{
  ival = int(x) # integer part, int() truncates

  # see if fractional part
  if (ival == x) # no fraction
    return ival # ensure no decimals

  if (x < 0) {
    aval = -x
    ival = int(aval)
    fraction = aval - ival
    if (fraction >= .5)
      return int(x) - 1 # -2.5 --> -3
    else
      return int(x) # -2.3 --> -2
    } else {
     fraction = x - ival
     if (fraction >= .5)
      return ival + 1
     else
      return ival
   }
 }
