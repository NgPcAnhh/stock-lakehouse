import http.server
import json
import subprocess
import threading
import uuid

PORT = 8088
tasks = {}

class CommandServer(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/submit":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            req = json.loads(post_data.decode('utf-8'))
            
            command = req.get("command")
            if not command:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing command parameter")
                return
            
            task_id = str(uuid.uuid4())
            tasks[task_id] = {
                "status": "RUNNING",
                "log": "",
                "exit_code": None
            }
            
            # Run the command in a background thread
            def run_task(tid, cmd):
                try:
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        shell=True,
                        text=True
                    )
                    log_output = []
                    while True:
                        line = process.stdout.readline()
                        if not line:
                            break
                        log_output.append(line)
                        # Keep only the last 5000 lines of logs to save memory
                        if len(log_output) > 5000:
                            log_output.pop(0)
                    process.wait()
                    tasks[tid]["status"] = "FINISHED" if process.returncode == 0 else "FAILED"
                    tasks[tid]["exit_code"] = process.returncode
                    tasks[tid]["log"] = "".join(log_output)
                except Exception as e:
                    tasks[tid]["status"] = "FAILED"
                    tasks[tid]["exit_code"] = -1
                    tasks[tid]["log"] = str(e)
            
            threading.Thread(target=run_task, args=(task_id, command)).start()
            
            self.send_response(202)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"task_id": task_id}).encode('utf-8'))
            
    def do_GET(self):
        if self.path.startswith("/status/"):
            task_id = self.path.split("/")[-1]
            if task_id not in tasks:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Task not found")
                return
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(tasks[task_id]).encode('utf-8'))

def run():
    server_address = ('', PORT)
    httpd = http.server.HTTPServer(server_address, CommandServer)
    print(f"Starting command server on port {PORT}...")
    httpd.serve_forever()

if __name__ == '__main__':
    run()
