import socket
import os
import sys
import json
import requests
import hashlib
import time

CONFIG_FILE = "config.json"

# ---------------- CONFIG VALIDATION ----------------
if not os.path.exists(CONFIG_FILE):
    print("❌ config.json not found.")
    sys.exit(1)

with open(CONFIG_FILE) as f:
    config = json.load(f)

NAMENODE_IP = config["namenode"]["ip"]
NAMENODE_API_PORT = 6000
NAMENODE_URL = f"http://{NAMENODE_IP}:{NAMENODE_API_PORT}"


# ---------------- UPLOAD ----------------
def upload(filename, progress_store=None):
    """Uploads a file to NameNode API."""
    if not os.path.exists(filename):
        msg = f"❌ File '{filename}' not found."
        print(msg)
        if progress_store is not None:
            progress_store[os.path.basename(filename)] = {"progress": 100, "message": msg}
        return

    basename = os.path.basename(filename)
    url = f"{NAMENODE_URL}/upload"

    if progress_store is not None:
        progress_store[basename] = {"progress": 0, "message": "🚀 Starting upload..."}

    print(f"\n🚀 Uploading '{basename}' to NameNode at {NAMENODE_URL} ...")
    try:
        with open(filename, "rb") as f:
            files = {"file": (basename, f)}
            resp = requests.post(url, files=files, timeout=60)

        msg = "✅ Upload complete!" if resp.status_code == 200 else "❌ Upload failed."
        print(msg)
        if progress_store is not None:
            progress_store[basename] = {"progress": 100, "message": msg}

    except Exception as e:
        msg = f"❌ Error: {e}"
        print(msg)
        if progress_store is not None:
            progress_store[basename] = {"progress": 100, "message": msg}


# ---------------- FETCH CHUNK ----------------
def fetch_chunk_from_datanode(dn_id, chunk_id):
    """Fetch a specific chunk from a DataNode using sockets."""
    dn = next((d for d in config["datanodes"] if d["id"] == dn_id), None)
    if not dn:
        print(f"⚠️ DataNode '{dn_id}' not found in config.")
        return None

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((dn["ip"], dn["port"]))
        s.send(f"GET:{chunk_id}\n".encode())

        data = b""
        while True:
            chunk = s.recv(2048)
            if not chunk:
                break
            if chunk.endswith(b"<END>"):
                data += chunk[:-5]
                break
            data += chunk
        s.close()

        if len(data) == 0:
            print(f"⚠️ Empty data from {dn_id} for {chunk_id}")
            return None

        checksum = hashlib.sha256(data).hexdigest()
        print(f"   ✅ Got {chunk_id} from {dn_id} [SHA256: {checksum[:8]}...]")
        return data

    except Exception as e:
        print(f"   ⚠️ Failed to fetch {chunk_id} from {dn_id}: {e}")
        return None


# ---------------- DOWNLOAD ----------------
def download(filename, progress_store=None):
    """Reconstruct a file from chunks across DataNodes (with failover)."""
    try:
        r = requests.get(f"{NAMENODE_URL}/api/metadata", timeout=5)
        metadata = r.json()
    except Exception as e:
        msg = f"❌ Could not fetch metadata: {e}"
        print(msg)
        if progress_store is not None:
            progress_store[filename] = {"progress": 100, "message": msg}
        return

    # Handle if metadata uses path key
    if filename not in metadata and f"uploads/{filename}" in metadata:
        filename = f"uploads/{filename}"

    if filename not in metadata:
        msg = f"❌ File '{filename}' not found in metadata."
        print(msg)
        if progress_store is not None:
            progress_store[filename] = {"progress": 100, "message": msg}
        return

    chunks_info = metadata[filename]["chunks"]
    os.makedirs("reconstructed", exist_ok=True)
    output_file = os.path.join("reconstructed", f"downloaded_{os.path.basename(filename)}")

    if os.path.exists(output_file):
        os.remove(output_file)

    print(f"\n📥 Reconstructing '{filename}' from DataNodes...")
    total = len(chunks_info)
    success_count = 0

    for i, chunk in enumerate(chunks_info, start=1):
        chunk_id = chunk["id"]
        replicas = chunk["replicas"]

        chunk_data = None
        for dn_id in replicas:
            chunk_data = fetch_chunk_from_datanode(dn_id, chunk_id)
            if chunk_data:
                # Inform dashboard about which DN served this chunk
                try:
                    requests.post(f"{NAMENODE_URL}/api/update_chunk_status",
                                  json={"chunk_id": chunk_id, "active_dn": dn_id},
                                  timeout=2)
                except:
                    pass
                break
            else:
                print(f"   ⚠️ {dn_id} failed. Trying next replica...")

        if chunk_data:
            with open(output_file, "ab") as out:
                out.write(chunk_data)
            success_count += 1
            print(f"   🔹Merged {chunk_id} ({i}/{total})")
        else:
            print(f"   ❌ All replicas failed for {chunk_id}")

        progress = int((i / total) * 100)
        if progress_store is not None:
            progress_store[os.path.basename(filename)] = {
                "progress": progress,
                "message": f"🔹Merged {chunk_id} ({i}/{total})"
            }

    msg = (f"✅ Download complete: {output_file}"
           if success_count == total
           else f"⚠️ Download finished with {total - success_count} missing chunks.")
    print(msg)

    if progress_store is not None:
        progress_store[os.path.basename(filename)] = {"progress": 100, "message": msg}


# ---------------- MAIN ----------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("\nUsage:\n  python3 cli.py upload <filename>\n  python3 cli.py download <filename>")
        sys.exit(1)

    cmd, filename = sys.argv[1].lower(), sys.argv[2]
    if cmd == "upload":
        upload(filename)
    elif cmd == "download":
        download(filename)
    else:
        print("❌ Invalid command. Use 'upload' or 'download'.")
