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

# Formas de verificar eficácia no paralelismo:
Temos o benchmark.py que realiza um run do pipeline algumas vezes e retorna um csv com os resultados por execução

Na minha máquina tive os seguintes resultados com 60 segundos para a geração de dados
```
Resumo final:
 - 1ª execução | 1 processo(s) → 10.6s
 - 1ª execução | 2 processo(s) → 7.81s
 - 1ª execução | 3 processo(s) → 7.88s
 - 1ª execução | 4 processo(s) → 6.92s
 ```
Para verificar diferentes configurações basta alterar as variáveis no topo da página
```
REPETICOES_POR_NPROC = 1
PROCESSOS_TESTADOS = [1, 2, 3, 4]
TIMEMOCK = 60
```
Entretanto acredito ser difícil fazer uma boa avaliação com essa file que vá além de 2 processos pois na nossa abordagem rodar
com muitos processos e um chunksize pequeno pode influenciar no resultado final

Outra forma é o teste manual, ative os mocks na pasta `mock` e rode o arquivo `src/Pipeline.py` modificando os valores de 

```
DEFAULT_NUM_PROCESSES = 4
CHUNK_SIZE = 5_000
```
