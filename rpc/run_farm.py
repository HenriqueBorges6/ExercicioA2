import subprocess, sys, os, time, statistics, json, multiprocessing as mp

SIM = "simulator.py"
SERVER = os.getenv("SERVER", "localhost:50051")

def run_instance(i, out_q):
    env = os.environ.copy()
    env["GRPC_SERVER"] = SERVER
    env["ID"] = str(i)
    p = subprocess.Popen([sys.executable, SIM], env=env,
                         stdout=subprocess.PIPE, text=True)
    lat = []
    for line in p.stdout:
        if line.startswith("[LAT]"):
            lat.append(float(line.split()[1]))  # em ms
    p.wait()
    out_q.put(lat)

def experiment(n_instances):
    q = mp.Queue()
    procs = [mp.Process(target=run_instance, args=(i,q)) for i in range(n_instances)]
    for p in procs: p.start()
    lat_all = []
    for _ in procs:
        lat_all.extend(q.get())
    for p in procs: p.join()
    return statistics.mean(lat_all)

if __name__ == "__main__":
    results = {}
    for n in [1,2,4,6,8,10,12,14,16,18,20]:
        print(f"▶ Rodando com {n} instâncias…")
        m = experiment(n)
        results[n] = m
        print(f"   média = {m:.1f} ms")
    with open("latency_results.json","w") as f:
        json.dump(results,f,indent=2)
