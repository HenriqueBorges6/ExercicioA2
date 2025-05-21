import os
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def remover_pasta(pasta):
    path = os.path.join(ROOT_DIR, pasta)
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f" Pasta removida: {pasta}")
    else:
        print(f" Pasta não encontrada: {pasta}")

def remover_arquivo(nome):
    path = os.path.join(ROOT_DIR, nome)
    if os.path.exists(path):
        os.remove(path)
        print(f" Arquivo removido: {nome}")
    else:
        print(f" Arquivo não encontrado: {nome}")

def main():
    print(" Resetando estado do sistema...")

    # 1. Limpa logs e diretório archive
    remover_pasta("streaming_logs")

    # 2. Limpa dados transformados
    remover_pasta("transformed_data")

    # 3. Remove banco de dados
    remover_arquivo("streaming_mock.db")

    # 5. Remove marcadores de progresso (se existirem)
    marcador_dir = os.path.join(ROOT_DIR, "src", "markers")
    if os.path.exists(marcador_dir):
        for f in os.listdir(marcador_dir):
            if f.endswith(".marker"):
                remover_arquivo(os.path.join("src", "markers", f))
    else:
        print(" Pasta de marcadores não encontrada.")

    print(" Estado resetado com sucesso.")

if __name__ == "__main__":
    main()
