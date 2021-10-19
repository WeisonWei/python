from __future__ import print_function

import re

a_regex = "Sync of transaction \-range .* took .*ms"
string = "Sync of transaction -range abc123 took 10123asdms"
a_pattern = re.compile(a_regex)
search = a_pattern.search(string)
if search is None:
    print("false")
else:
    print("true")
group = search.group()
print("=======end==========")
