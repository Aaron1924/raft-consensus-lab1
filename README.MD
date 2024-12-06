# INSTRUCTION FOR THE PROJECT

1. **Create a new repository on GitHub and clone it to your computer**

2. **install dependencies, make sure you have python installed on your computer**
```bash
pip install -r requirements.txt
```

3. **Install and connect MongoDB** 

You can install MongoDB via [this](https://www.mongodb.com/try/download/community-kubernetes-operator), choose your appropriate OS and follow the instructions. 

After installing MongoDB, you can connect to the MongoDB *localhost* ( default is 27017) server via MongoDB Compass or MongoDB shell.


4. **Run the server on the terminal, you can create it by run on new terminal , config the server by changing the *config.conf* file**
```bash
python server.py {id}
```

5. **Run the client**
```bash
python client.py
```

6. **Run the client with a command**
```bash
connect {server_ip_addr} {server_port}: connect to a server node by its IP address and port.
 eg. connect localhost 5000

getleader: asks current server which node is the current leader.

setval {key} {value}: put a key value pair into the storage being connected to some server.

getval {key}: get a value by a key being connected to some server. None will be returned if no such a key exists.

suspend {sec}: suspend the current server node on the given number of seconds. If you suspend a leader node, the re-election process will start in an amount of time.

quit: quits client.
```

