from flask import Flask, jsonify, request
import json
import os
import socket
import time

app = Flask(__name__)

METADATA_FILE = "namenode_metadata.json"
CONFIG_FILE = "config.json"

with open(CONFIG_FILE) as f:
    config = json.load(f)

NAMENODE_IP = config["namenode"]["ip"]
NAMENODE_PORT = config["namenode"]["port"]

# ------------------ HELPERS ------------------
def read_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}

# ------------------ ROUTES ------------------
@app.route("/api/metadata", methods=["GET"])
def api_metadata():
    return jsonify(read_metadata())

@app.route("/upload", methods=["POST"])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Empty filename"}), 400

    upload_path = os.path.join("uploads", file.filename)
    os.makedirs("uploads", exist_ok=True)
    file.save(upload_path)

    print(f"🚀 Uploading {upload_path} to NameNode socket ({NAMENODE_IP}:{NAMENODE_PORT})...")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((NAMENODE_IP, NAMENODE_PORT))

    # ✅ Header ends with newline
    s.sendall(f"UPLOAD:{file.filename}\n".encode())

    with open(upload_path, "rb") as f:
        while chunk := f.read(2048):
            s.sendall(chunk)
    s.sendall(b"<END>")
    s.close()

    print(f"✅ File '{file.filename}' sent successfully to Namenode.py")
    return jsonify({"message": f"File '{file.filename}' uploaded successfully"}), 200

@app.route("/", methods=["GET"])
def home():
    return "<h3>✅ NameNode API active. Dashboard connected.</h3>"

if __name__ == "__main__":
    print("🌐 Starting NameNode API on port 6000...")
    app.run(host="0.0.0.0", port=6000)