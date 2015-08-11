__author__ = 'Zakaria'

import random
header = ["R", "S"]
filename = "data.txt"
target = open(filename, 'w')
index_r = random.sample(range(1, 70), 60)
index_s = index_r[:]

for i in range(0, 100):
    h = random.choice(header)
    target.write(h + '\n')
    if h == "R":
        target.write(str(index_r.pop()) + '\n')
    else:
        target.write(str(index_s.pop()) + '\n')
    target.write(chr(random.randint(97, 122)) +
                 chr(random.randint(97, 122)) +
                 chr(random.randint(97, 122)) +
                 '\n')


target.close()
