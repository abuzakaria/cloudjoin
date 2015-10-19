__author__ = 'Zakaria'

import random
header = ["R", "S"]
filename = "data.txt"
target = open(filename, 'w')
index_r = list(range(1, 200))
index_s = index_r[:]

for i in range(0, 200):
    h = random.choice(header)
    target.write(h + '\n')
    if h == "R":
        target.write(str(index_r.pop(0)) + '\n')
    else:
        target.write(str(index_s.pop(0)) + '\n')
    target.write(chr(random.randint(97, 100)) +
                 '\n')


target.close()
