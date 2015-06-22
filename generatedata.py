__author__ = 'Zakaria'

import random
header = ["R", "S"]
filename = "data.txt"
target = open(filename, 'w')
index_r = random.sample(range(1, 70), 50) + random.sample(range(1, 70), 50)

for i in range(0, 100):
    h = random.choice(header)
    target.write(h + '\n')
    target.write(str(index.pop()) + '\n')
    target.write(chr(random.randint(97, 122)) +
                 chr(random.randint(97, 122)) +
                 chr(random.randint(97, 122)) +
                 '\n')


target.close()
