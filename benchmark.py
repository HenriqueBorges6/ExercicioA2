import subprocess
import time
import csv
import os
import signal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MOCK_STREAM = os.path.join(SCRIPT_DIR, "mock", "mock.py")
MOCK_DB     = os.path.join(SCRIPT_DIR, "mock", "mock_db.py")
PIPELINE    = os.path.join(SCRIPT_DIR, "src", "Pipeline.py")
RESET       = os.path.join(SCRIPT_DIR, "src", "reset_state.py")
RESULT_CSV  = "benchmark_results.csv"

REPETICOES_POR_NPROC = 1
PROCESSOS_TESTADOS = [1, 2, 3, 4]
TIMEMOCK = 60
mock_processes = {}

def executar_reset():
    print("🔄 Resetando estado...")
    subprocess.run(["python", RESET], check=True)

def iniciar_mock(nome, path):
    proc = subprocess.Popen(
        ["python", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    mock_processes[nome] = proc
    print(f" Mock '{nome}' iniciado (PID={proc.pid})")

def encerrar_mocks():
    for nome, proc in mock_processes.items():
        print(f" Encerrando mock '{nome}'...")
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
    mock_processes.clear()

def executar_pipeline(nproc: int) -> float:
    print(f" Executando pipeline com {nproc} processo(s)...")
    start = time.time()
    try:
        subprocess.run(["python", PIPELINE, str(nproc)], check=True)
    except subprocess.CalledProcessError as e:
        print(f" Falha ao executar pipeline com {nproc} processos.")
        return -1
    return round(time.time() - start, 2)

def salvar_resultados(resultados):
    with open(RESULT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["processos", "execucao", "tempo_segundos"])
        writer.writerows(resultados)
    print(f"\n Resultados salvos em {RESULT_CSV}")

def main():
    resultados = []

    for n in PROCESSOS_TESTADOS:
        for execucao in range(1, REPETICOES_POR_NPROC + 1):
            print(f"\n Execução {execucao} com {n} processo(s)")
            executar_reset()
            iniciar_mock("mock_stream", MOCK_STREAM)
            iniciar_mock("mock_db", MOCK_DB)
            print(f" Aguardando {TIMEMOCK} segundos para geração de dados...")
            time.sleep(TIMEMOCK)
            tempo = executar_pipeline(n)
            encerrar_mocks()
            if tempo > 0:
                resultados.append([n, execucao, tempo])
            time.sleep(1)

    salvar_resultados(resultados)

    print("\n Resumo final:")
    for linha in resultados:
        print(f" - {linha[1]}ª execução | {linha[0]} processo(s) → {linha[2]}s")

if __name__ == "__main__":
    main()
