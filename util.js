
export let expBackoff = (startMs, { repeatAfter = 5, numEntries = 5 }) => {
  let vals = []
  let val = startMs
  for (let i = 0; i < numEntries; i++) {
    vals.push(val)
    val = i === repeatAfter ? val : val * 2
  }
  return vals
}

