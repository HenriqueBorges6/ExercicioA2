# A1-Scalable-Computing

## Como Rodar o Projeto (Usando PowerShell)

Siga estas etapas para configurar e rodar o projeto:

1. **Crie e ative um ambiente virtual (opcional, mas recomendado):**
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

2. **Instale os pacotes necessários:**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Obtenha o IP da sua máquina local (necessário para o cliente se conectar ao servidor gRPC):**

   No **macOS**, execute:
   ```bash
   ipconfig getifaddr en0
   ```

   Copie o IP retornado e defina a variável `GRPC_SERVER`. Exemplo:
   ```bash
   export GRPC_SERVER=192.168.15.104:50051
   ```

4. **Inicie o servidor gRPC (`grpc_server.py`) — este passo deve ser feito primeiro:**
   ```powershell
   python grpc_server.py
   ```

5. **Com o servidor já em execução, inicie o gerador de eventos (`simulator.py`):**
   ```powershell
   python simulator.py
   ```

   Após o início, pressione `P` para ativar o envio dos eventos simulados, ou aguarde o envio automático.

6. **Rode o dashboard (`dashboard.py`) para visualizar os dados:**
   ```powershell
   streamlit run dashboard.py
   ```

### Certifique-se de manter o servidor ativo enquanto roda o `simulator.py` e o dashboard.
