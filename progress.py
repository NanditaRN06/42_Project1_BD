# dashboard.py
from flask import Flask, render_template_string, request, jsonify, Response
import os
import json
import threading
import requests
import time
import cli  # your upload/download logic

progress_store = {}   # stores progress for each filename

# ---------------- CONFIG ----------------
with open("config.json") as f:
    config = json.load(f)

NAMENODE_IP = config["namenode"]["ip"]
DATANODES = config["datanodes"]

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__, static_folder=UPLOAD_FOLDER)

# ---------------- TEMPLATE ----------------
TEMPLATE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Mini HDFS — Real-time Dashboard</title>
<style>
body{font-family:Inter,Segoe UI,Arial; background:#f4f6fb; color:#222; padding:20px;}
.wrap{max-width:1100px;margin:0 auto;}
h1{color:#0b57a4}
.card{background:#fff;padding:14px;border-radius:8px;box-shadow:0 4px 12px rgba(20,30,60,0.06);margin-bottom:16px;}
table{width:100%;border-collapse:collapse}
th,td{padding:8px;border-bottom:1px solid #eee;text-align:left}
th{background:#0b57a4;color:#fff}
.btn{background:#0b57a4;color:#fff;padding:7px 12px;border:none;border-radius:6px;cursor:pointer}
.status-green{color:green;font-weight:700}
.status-red{color:red;font-weight:700}
.toast{position:fixed;right:20px;bottom:30px;background:#111;color:#fff;padding:12px 16px;border-radius:6px;opacity:0;transform:translateY(10px);transition:all .35s}
.toast.show{opacity:1;transform:translateY(0)}
.chunk-pill{display:inline-block;padding:4px 8px;border-radius:6px;color:#fff;margin:2px;font-size:12px}
.dn1{background:#1f78d1}
.dn2{background:#28a745}
.dn3{background:#f0ad4e;color:#000}
.progress-wrap{width:100%;background:#eee;border-radius:6px;margin-top:10px;}
.progress-bar{height:10px;width:0%;background:#0b57a4;border-radius:6px;transition:width 0.2s;}
</style>
</head>
<body>

<div class="wrap">
  <h1>Mini HDFS — Real-time Dashboard</h1>

  <div class="card" id="uploadCard">
    <b>Upload file</b>
    <form id="uploadForm" style="margin-top:10px">
      <input type="file" id="fileInput" name="file" required>
      <button class="btn" type="submit">Upload (to Namenode)</button>
    </form>
  </div>

  <div class="card">
    <b>DataNode Status</b>
    <table id="datanodeTable" style="margin-top:10px">
      <thead><tr><th>ID</th><th>IP</th><th>Port</th><th>Status</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="card">
    <b>Files & Chunk Distribution</b>
    <table id="filesTable" style="margin-top:10px">
      <thead><tr><th>File</th><th>#Chunks</th><th>Distribution</th><th>Action</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>

  <div style="font-size:12px;color:#666">Auto-update: every 3 seconds. Last: <span id="lastUpdate">-</span></div>
</div>

<div id="toast" class="toast"></div>

<script>
// ---------------- Toast ----------------
const toastEl = document.getElementById('toast');
function showToast(msg, ms=3500){
  toastEl.textContent = msg;
  toastEl.classList.add('show');
  setTimeout(()=> toastEl.classList.remove('show'), ms);
}

// ---------------- Progress Bar UI ----------------
function createProgressUI(filename){
  const card = document.getElementById('uploadCard');
  const container = document.createElement('div');
  container.className = 'progress-wrap';

  const label = document.createElement('div');
  label.style.marginTop = '10px';
  label.style.fontWeight = '600';
  label.textContent = filename;

  const bar = document.createElement('div');
  bar.className = 'progress-bar';
  bar.id = 'bar-' + filename;

  const msg = document.createElement('div');
  msg.id = 'msg-' + filename;
  msg.style.fontSize = '13px';
  msg.style.marginTop = '4px';

  // Wrapper
  const wrap = document.createElement('div');
  wrap.style.width = '100%';
  wrap.style.background = '#eee';
  wrap.style.borderRadius = '6px';
  wrap.style.overflow = 'hidden';
  wrap.appendChild(bar);

  container.appendChild(label);
  container.appendChild(wrap);
  container.appendChild(msg);
  card.appendChild(container);

  return container;
}

// ---------------- SSE Stream ----------------
function startProgressStream(filename) {
  const container = createProgressUI(filename);

  const evt = new EventSource(`/progress/${filename}`);
  evt.onmessage = (e) => {
    const data = JSON.parse(e.data);
    const bar = document.getElementById('bar-' + filename);
    const msg = document.getElementById('msg-' + filename);

    bar.style.width = data.progress + '%';
    msg.textContent = data.message;

    if (data.progress >= 100) {
      evt.close();
      showToast('✅ ' + data.message);
      setTimeout(() => container.remove(), 2000);
    }
  };
}

// ---------------- Fetch helpers ----------------
async function fetchJSON(path){
  const r = await fetch(path);
  return await r.json();
}

// ---------------- Render tables ----------------
function renderDatanodes(list){
  const tbody = document.querySelector('#datanodeTable tbody');
  tbody.innerHTML = '';
  list.forEach(dn=>{
    const tr=document.createElement('tr');
    tr.innerHTML = `
      <td>${dn.id}</td>
      <td>${dn.ip}</td>
      <td>${dn.port}</td>
      <td class="${dn.status==='🟢'?'status-green':'status-red'}">${dn.status}</td>`;
    tbody.appendChild(tr);
  });
}

function renderFiles(files){
  const tbody = document.querySelector('#filesTable tbody');
  tbody.innerHTML = '';
  if(files.length===0){
    tbody.innerHTML = '<tr><td colspan="4" style="color:#666">No files uploaded yet</td></tr>';
    return;
  }
  files.forEach(f=>{
    const tr=document.createElement('tr');
    const dist = f.chunks.map(c =>
      `<div><strong>${c.id}</strong> →
         ${c.replicas.map((r,i)=>`<span class="chunk-pill dn${(i%3)+1}">${r}</span>`).join(' ')}
       </div>`
    ).join('');
    tr.innerHTML = `
      <td>${f.filename}</td>
      <td>${f.num_chunks}</td>
      <td>${dist}</td>
      <td><button class="btn" onclick="startDownload('${encodeURIComponent(f.filename)}')">Download</button></td>`;
    tbody.appendChild(tr);
  });
}

// ---------------- Upload ----------------
document.getElementById('uploadForm').addEventListener('submit', async e=>{
  e.preventDefault();
  const fileInput = document.getElementById('fileInput');
  if(!fileInput.files.length){
    showToast('Select a file first');
    return;
  }

  const fd = new FormData();
  fd.append('file', fileInput.files[0]);
  const filename = fileInput.files[0].name;

  showToast('📤 Upload started...');
  startProgressStream(filename);

  await fetch('/upload', { method: 'POST', body: fd });
});

// ---------------- Download ----------------
async function startDownload(fnEnc){
  const filename = decodeURIComponent(fnEnc);
  showToast('📥 Download started...');
  startProgressStream(filename);
  await fetch(`/download/${filename}`, { method:'POST' });
}

// ---------------- Poll metadata ----------------
async function pollAll(){
  try{
    const [meta,dns] = await Promise.all([
      fetchJSON('/api/metadata'),
      fetchJSON('/api/datanodes')
    ]);

    const files = Object.entries(meta).map(([fname,info])=>{
      const chunks = Array.isArray(info.chunks)
        ? info.chunks.map(c => ({id: c.id, replicas: c.replicas}))
        : Object.entries(info.chunks||{}).map(([cid,locs])=>({id:cid,replicas:locs}));
      return {filename:fname, num_chunks:chunks.length, chunks};
    });

    renderFiles(files);
    renderDatanodes(dns);
    document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();

  }catch(e){console.error(e);}
  finally{ setTimeout(pollAll,3000); }
}
pollAll();

</script>
</body>
</html>
"""

# ---------------- HELPERS ----------------
def check_datanode_status(ip, port):
    import socket
    s = socket.socket()
    s.settimeout(0.8)
    try:
        s.connect((ip, port))
        s.close()
        return "🟢"
    except:
        return "🔴"

def get_metadata_live():
    try:
        r = requests.get(f"http://{NAMENODE_IP}:6000/api/metadata", timeout=1)
        if r.status_code == 200:
            return r.json()
    except:
        return {}
    return {}

# ---------------- API ROUTES ----------------
@app.route("/api/metadata")
def api_metadata():
    return jsonify(get_metadata_live())

@app.route("/api/datanodes")
def api_datanodes():
    data=[]
    for dn in DATANODES:
        data.append({
            "id": dn["id"],
            "ip": dn["ip"],
            "port": dn["port"],
            "status": check_datanode_status(dn["ip"], dn["port"])
        })
    return jsonify(data)

@app.route("/upload", methods=["POST"])
def upload_file():
    f = request.files.get("file")
    if not f:
        return "No file uploaded", 400

    filepath = os.path.join(UPLOAD_FOLDER, f.filename)
    f.save(filepath)

    progress_store[f.filename] = {"progress": 0, "message": "📤 Uploading..."}

    threading.Thread(
        target=lambda: cli.upload(filepath, progress_store),
        daemon=True
    ).start()

    return jsonify({"status": "started", "filename": f.filename})

@app.route("/download/<path:filename>", methods=["POST"])
def download_file(filename):
    progress_store[filename] = {"progress": 0, "message": f"📥 Downloading {filename}..."}

    threading.Thread(
        target=lambda: cli.download(filename, progress_store),
        daemon=True
    ).start()

    return jsonify({"status": "started", "filename": filename})

# ✅ SERVER-SENT EVENTS STREAM
@app.route("/progress/<path:filename>")
def progress_stream(filename):
    def generate():
        while True:
            if filename in progress_store:
                data = progress_store[filename]
                yield f"data: {json.dumps(data)}\n\n"

                if data["progress"] >= 100:
                    break
            time.sleep(0.5)
    return Response(generate(), mimetype="text/event-stream")

@app.route("/")
def index():
    return render_template_string(TEMPLATE)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("🚀 Dashboard running at http://localhost:8080")
    app.run(host="0.0.0.0", port=8080)
