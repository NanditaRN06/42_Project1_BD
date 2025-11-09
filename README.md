# Mini HDFS Simulation-Exploring Distributed Data Storage

**Team - 42**

A minimal, multi-threaded distributed file system implementation in Python using sockets for communication, featuring a NameNode, multiple DataNodes, file chunking, replication, and basic health checks (heartbeats, re-replication) and data integrity (checksums).

## Folder structure

```
42_Project1_BD/
├── cli.py
├── namenode.py
├── namenode_api.py
├── datanode1.py
├── datanode2.py
├── progress.py
├── README.md        
```

## Distributed System

The Distributed File System is be deployed across 4 separate machines connected via a secure ZeroTier virtual network. All internal communication will use the managed IP addresses assigned by ZeroTier.

1. **Machine - 1** - NameNode (`namenode.py`, `namenode-api.py`)
1. **Machine - 2** - DataNode (`datanode1.py`)
1. **Machine - 3** - DataNode (`datanode2.py`)
1. **Machine - 4** - Client (`cli.py`, `progress.py`)

## Prequisites

- **Python 3.x**: Installed on all machines.
- **Python Libraries**: The `flask` and `requests` libraries are required on Machine 1 and Machine 4 for the API and client interaction.
```
pip install flask requests
```
- **ZeroTier Account**: An account on the ZeroTier Central website.
- **ZeroTier Client**: The ZeroTier client software (ZeroTier One) must be installed on all 4 machines.

## Set up and Run
1. **ZeroTier Network Configuration**
    - Create a new virtual network on ZeroTier Central and obtain the Network ID.
    - Install the ZeroTier client on all 4 physical machines and use the Network ID to join the virtual network.
    - On ZeroTier Central, go to the Members list and authorize all 4 machines. Note the Managed IP Address (ex: `192.168.194.XXX`) assigned to each machine.
    - ping each other to confirm connectivity.
```
ping 192.168.194.XXX
```

2. **File and Configuration Preparation**
    - Store the programs in their respective machines.
    - Create `config.json` on all 4 machines, in the same directory as the Python files. Replace the IP placeholders with the ZeroTier Managed IP Address.
  
    ```
    sudo nano config.json

    # config.json template
    {
    "namenode": { "ip": <Add ip address>, "port": 5001 }, 
    "datanodes": [
        { "id": "DN1", "ip": <Add ip address>, "port": 5002 }, 
        { "id": "DN2", "ip": <Add ip address>, "port": 5003 } 
    ],
    "client": { "ip": <Add ip address>, "port": 5000 } 
    }

    ```

3. **Run the System**

The DataNodes must be started before the NameNode to ensure heartbeats are received immediately, and the NameNode must be running before the Client attempts communication.

**Machine 2 & 3 (DataNodes)**
```
# Machine - 2
python3 datanode1.py

# Machine - 3
python3 datanode2.py
```

**Machine 1 (Namenode)**
```
# Start the core NameNode logic
python3 namenode.py

# (In another terminal): Start the API for HTTP uploads
python3 namenode_api.py
```

**Machine 4 (Client)**
```
# Start the client CLI for interaction
python3 cli.py

# (In another terminal): Start the monitoring dashboard
python3 progress.py
```

## Contributors
- [Nandita R Nadig](https://www.github.com/NanditaRN06)
- [Moulya K A](https://www.github.com/moulyajs)
- [Najmus Seher](https://www.github.com/Sehar-12)
- [Navyashree SP](https://www.github.com/navya-bit)
