from collections import defaultdict
d = defaultdict(list)
with open('latency.log', 'r') as f:
	for line in f:
		d[int(line.split()[0])].append(int(line.split()[1]))

for k,v in d.items():
	d[k] = int(round(sum(v) / len(v), 0))
with open('latency2.log', 'w') as f:
	for k,v in sorted(d.items()):
		f.write(str(k) + '\t' + str(v) +'\n')
