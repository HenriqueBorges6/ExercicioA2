# A1-Scalable-Computing

## Como Rodar o Projeto (Usando PowerShell)

Siga estas etapas para configurar e rodar o projeto:

1. **Crie e ative um ambiente virtual (opcional, mas recomendado):**
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
2. **Instale os pacotes necessários**
    ```powershell
    pip install -r requirements.txt
3. **Inicie o servidor**
    ```powershell
    python server.py
4. **Somente após iniciar o servidor, inicie o dashboard.py**
    ```powershell
    streamlit run dashboard.py
