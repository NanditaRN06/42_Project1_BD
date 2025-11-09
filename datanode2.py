import socket
import threading
import json
import os
import time
import hashlib

# ------------------ LOAD CONFIG ------------------
with open("config.json") as f:
    config = json.load(f)

DN_ID = "DN2"  # Change to DN2 for datanode2
namenode_ip = config["namenode"]["ip"]
namenode_port = config["namenode"]["port"] + 100  # Heartbeat port

this_node = next(dn for dn in config["datanodes"] if dn["id"] == DN_ID)
DATA_PATH = f"storage_{DN_ID}"
os.makedirs(DATA_PATH, exist_ok=True)

CHECKSUM_FILE = os.path.join(DATA_PATH, "checksums.json")
if os.path.exists(CHECKSUM_FILE):
    with open(CHECKSUM_FILE, "r") as f:
        checksums = json.load(f)
else:
    checksums = {}

def save_checksums():
    with open(CHECKSUM_FILE, "w") as f:
        json.dump(checksums, f, indent=4)

# ------------------ HEARTBEAT ------------------
def send_heartbeat():
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((namenode_ip, namenode_port))
            s.send(f"HEARTBEAT:{DN_ID}".encode())
            s.close()
        except:
            pass
        time.sleep(5)

# ------------------ DATA LISTENER ------------------
def data_listener():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((this_node["ip"], this_node["port"]))
    s.listen()
    print(f"📦 {DN_ID} listening on {this_node['ip']}:{this_node['port']}")

    while True:
        conn, addr = s.accept()
        try:
            header = b""
            while not header.endswith(b"\n"):
                part = conn.recv(1)
                if not part:
                    break
                header += part
            header = header.decode(errors="ignore").strip()

            if not header:
                continue

            if header.startswith("STORE:"):
                chunk_name = header.split(":", 1)[1]
                store_chunk(conn, chunk_name)
            elif header.startswith("GET:"):
                chunk_name = header.split(":", 1)[1]
                send_chunk(conn, chunk_name)
            else:
                print(f"⚠️ Unrecognized header from {addr}: {header!r}")

        except Exception as e:
            print(f"❌ Error handling connection from {addr}: {e}")
        finally:
            conn.close()

# ------------------ STORE CHUNK ------------------
def store_chunk(conn, chunk_name):
    data = bytearray()
    while True:
        chunk = conn.recv(2048)
        if not chunk:
            break
        if chunk.endswith(b"<END>"):
            data += chunk[:-5]
            break
        data += chunk

    file_path = os.path.join(DATA_PATH, chunk_name)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(data)

    # Compute SHA256 checksum
    checksum = hashlib.sha256(data).hexdigest()
    checksums[chunk_name] = checksum
    save_checksums()

    print(f"✅ {DN_ID}: Stored {chunk_name} ({len(data)} bytes, checksum {checksum})")

# ------------------ SEND CHUNK ------------------
def send_chunk(conn, chunk_name):
    path = os.path.join(DATA_PATH, chunk_name)
    if not os.path.exists(path):
        conn.send(b"<END>")
        return

    # Verify checksum before sending
    with open(path, "rb") as f:
        data = f.read()
        checksum = hashlib.sha256(data).hexdigest()
        if checksums.get(chunk_name) != checksum:
            print(f"❌ {DN_ID}: Checksum mismatch for {chunk_name}")
            conn.send(b"<END>")
            return

        conn.sendall(data)
        conn.sendall(b"<END>")

    print(f"📤 {DN_ID}: Sent {chunk_name} (verified)")

# ------------------ START THREADS ------------------
threading.Thread(target=send_heartbeat, daemon=True).start()
data_listener()


#datanode.py
