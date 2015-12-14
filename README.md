# cloudjoin
## Requirements
* VirtualBox
* Vagrant
* Python 3.4 (in host machine)

## System start
In terminal/console,  the following command will start the virtual machines.

```
vagrant up
```

The virtual machines for this prototype are:
* source
* p1
* p2
* p3
* dest

Using ssh these machines can be accessed by using the following command:
```
vagrant ssh <machine name>
```

After accessing these machines, use the following commands in appropriate virtual machines to run the system:
```
sudo python3 source.py 10.10.10.2 80
```
```
sudo python3 processor.py p1 10.10.10.3 80
```
```
sudo python3 processor.py p2 10.10.10.4 80 6 8 9 2 2 4
```
```
sudo python3 processor.py p3 10.10.10.5 80 6 8 9 2 2 4
```
```
sudo python3 destination.py 10.10.10.6 80
```

The interface can be another virtual machine or the host machine. Open another terminal and run the following command to enable the interface

```
python interface.py
```
## Modes
mode A 10.10.10.3 80 -3

mode B 127.0.0.1 12345 3 10.10.10.4 80

mode C 10.10.10.3 80

mode D 1 3 10.10.10.3 80 -5

mode E 10.10.10.3 80 3 10.10.10.4 80 -3

mode F 1 3 10.10.10.3 80 -3

mode G 10.10.10.3 80 10 10.10.10.4 80

mode H 1 10 10.10.10.3 80

mode I 1 10 10.10.10.3 80 -3

mode J 10.10.10.3 80 10

mode K 10.10.10.3 80 13 10.10.10.4 80

mode L 1 20
