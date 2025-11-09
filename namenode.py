import socket
import threading
import json
import os
import time

# ------------------ LOAD CONFIG ------------------
with open("config.json") as f:
    config = json.load(f)

NAMENODE_IP = config["namenode"]["ip"]
NAMENODE_PORT = config["namenode"]["port"]

metadata_file = "namenode_metadata.json"

if os.path.exists(metadata_file):
    with open(metadata_file, "r") as mf:
        metadata = json.load(mf)
else:
    metadata = {}

datanodes = {
    dn["id"]: {
        "ip": dn["ip"],
        "port": dn["port"],
        "alive": False,
        "last_heartbeat": 0
    } for dn in config["datanodes"]
}

def save_metadata():
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=4)
    print("🗂 Metadata updated in namenode_metadata.json")

# ------------------ HEARTBEAT LISTENER ------------------
def heartbeat_listener():
    hb_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    hb_socket.bind((NAMENODE_IP, NAMENODE_PORT + 100))
    hb_socket.listen()
    print(f"💓 Heartbeat listener active on port {NAMENODE_PORT + 100}")

    while True:
        conn, addr = hb_socket.accept()
        data = conn.recv(1024).decode()
        if data.startswith("HEARTBEAT:"):
            dn_id = data.split(":")[1]
            if dn_id in datanodes:
                datanodes[dn_id]["alive"] = True
                datanodes[dn_id]["last_heartbeat"] = time.time()
        conn.close()

# ------------------ MONITOR DEAD NODES ------------------
def monitor_nodes():
    while True:
        now = time.time()
        for dn_id, info in datanodes.items():
            if info["alive"] and now - info["last_heartbeat"] > 15:
                print(f"⚠️ {dn_id} considered dead")
                info["alive"] = False
                # Trigger re-replication for missing chunks
                re_replicate_chunks(dn_id)
        time.sleep(5)

def re_replicate_chunks(dead_dn):
    for fname, fdata in metadata.items():
        for chunk in fdata["chunks"]:
            if dead_dn in chunk["replicas"]:
                chunk["replicas"].remove(dead_dn)
            # If replicas < 2, replicate to another alive DataNode
            if len(chunk["replicas"]) < 2:
                for dn_id, info in datanodes.items():
                    if info["alive"] and dn_id not in chunk["replicas"]:
                        try:
                            # Fetch existing chunk from any replica
                            src_dn = chunk["replicas"][0]
                            chunk_data = fetch_chunk_from_datanode(src_dn, chunk["id"])
                            send_chunk_to_datanode(chunk_data, chunk["id"], dn_id)
                            chunk["replicas"].append(dn_id)
                            print(f"🔄 Re-replicated {chunk['id']} → {dn_id}")
                            break
                        except:
                            continue
    save_metadata()

# ------------------ FETCH CHUNK FROM DATANODE ------------------
def fetch_chunk_from_datanode(dn_id, chunk_name):
    info = datanodes[dn_id]
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((info["ip"], info["port"]))
    s.send(f"GET:{chunk_name}\n".encode())
    data = b""
    while True:
        chunk = s.recv(2048)
        if chunk.endswith(b"<END>"):
            data += chunk[:-5]
            break
        data += chunk
    s.close()
    return data

# ------------------ SEND CHUNK TO DATANODE ------------------
def send_chunk_to_datanode(chunk, chunk_name, dn_id):
    info = datanodes[dn_id]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((info["ip"], info["port"]))
        s.send(f"STORE:{chunk_name}\n".encode())
        time.sleep(0.05)
        s.sendall(chunk)
        s.sendall(b"<END>")
        s.close()
        print(f"📦 Sent {chunk_name} → {dn_id}")
        return True
    except Exception as e:
        print(f"❌ Failed to send {chunk_name} to {dn_id}: {e}")
        return False

# ------------------ CLIENT LISTENER ------------------
def client_listener():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((NAMENODE_IP, NAMENODE_PORT))
    s.listen()
    print(f"🧠 Namenode listening on {NAMENODE_IP}:{NAMENODE_PORT}")

    while True:
        conn, addr = s.accept()
        data = conn.recv(2048)
        try:
            header_str = data.decode()
            header, _sep, leftover = header_str.partition("\n")
        except:
            header = data.decode(errors="ignore")
            leftover = b""

        if header.startswith("UPLOAD:"):
            filename = header.split(":", 1)[1].strip()
            handle_upload(conn, filename, leftover.encode())
        elif header.startswith("DOWNLOAD:"):
            filename = header.split(":", 1)[1].strip()
            handle_download(conn, filename)
        conn.close()

# ------------------ HANDLE UPLOAD ------------------
def handle_upload(conn, filename, leftover=b""):
    data = leftover
    while True:
        chunk = conn.recv(2048)
        if chunk.endswith(b"<END>"):
            data += chunk[:-5]
            break
        data += chunk

    chunk_size = 2 * 1024 * 1024
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    metadata[filename] = {"chunks": []}

    for i, chunk in enumerate(chunks):
        chunk_name = f"{filename}_chunk{i}"
        replicas = []
        for dn_id, info in datanodes.items():
            if info["alive"] and len(replicas) < 2:
                if send_chunk_to_datanode(chunk, chunk_name, dn_id):
                    replicas.append(dn_id)
        metadata[filename]["chunks"].append({
            "id": chunk_name,
            "replicas": replicas
        })
    save_metadata()
    print(f"✅ File '{filename}' stored with {len(chunks)} chunks.")

# ------------------ HANDLE DOWNLOAD ------------------
def handle_download(conn, filename):
    if filename not in metadata:
        conn.send(b"File not found")
        return
    for chunk_info in metadata[filename]["chunks"]:
        chunk_name = chunk_info["id"]
        for dn_id in chunk_info["replicas"]:
            info = datanodes.get(dn_id)
            if info and info["alive"]:
                try:
                    dn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    dn_socket.connect((info["ip"], info["port"]))
                    dn_socket.send(f"GET:{chunk_name}\n".encode())
                    while True:
                        data = dn_socket.recv(2048)
                        if data == b"<END>":
                            break
                        conn.send(data)
                    dn_socket.close()
                    break
                except:
                    continue
    conn.send(b"<END>")

# ------------------ START THREADS ------------------
if __name__ == "__main__":
    threading.Thread(target=heartbeat_listener, daemon=True).start()
    threading.Thread(target=monitor_nodes, daemon=True).start()
    client_listener()


#namenode.py