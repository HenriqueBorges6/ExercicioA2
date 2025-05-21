import os
import shutil
import sqlite3
from DataFrame import DataFrame

STREAMING_LOG_DIR = "streaming_logs"
ARCHIVE_DIR = os.path.join(STREAMING_LOG_DIR, "archive")

class DataRepository:
    def __init__(self, db_path: str = None):
        """
        Inicializa o repositório de dados.

        Args:
            db_path: caminho para o arquivo SQLite. Se None, usa `../streaming_mock.db`.
        """
        base_dir = os.path.dirname(__file__)
        self.db_path = db_path or os.path.join(base_dir, '..', 'streaming_mock.db')
    
    def read_header(self, file_path):
        header_columns = []

        with open(file_path, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            if not header_line:
                raise ValueError("Arquivo de log não contém cabeçalho (linha vazia).") 
            header_columns = [h.strip() for h in header_line.split(',')]
            if not header_columns or not all(header_columns):
                raise ValueError(f"Cabeçalho inválido (contém partes vazias): {header_line}")
        
        return header_columns


    def _create_dataframe_from_chunk_lines(self, chunk_lines, header_columns):
        if not header_columns or not isinstance(header_columns, list) or not all(isinstance(c, str) for c in header_columns):
            print("Error: header_columns inválido fornecido para _create_dataframe_from_chunk_lines")
            return None

        chunk_df = DataFrame(columns=header_columns)
        num_expected_columns = len(header_columns)

        for line in chunk_lines:
            row_line = line.strip()
            if not row_line:
                continue
            
            row_values = [val.strip() for val in row_line.split(',')]
            
            if len(row_values) == num_expected_columns:
                try:
                    chunk_df.add_row(row_values)
                except Exception as e_add:
                    print(f"Erro ao adicionar linha ao DF do chunk: {row_values}, Erro: {e_add}")
                    pass 

        return chunk_df

    def load_content_metadata(self) -> DataFrame:
        query = "SELECT content_id, content_genre FROM Content"
        return self.execute_query_to_dataframe(query, expected_columns=['content_id', 'content_genre'])

    def process_new_log_files(self, chunk_size, task_queue):
        """Scans STREAMING_LOG_DIR for new .txt files, processes them in chunks, 
           queues DataFrames, and moves processed files to ARCHIVE_DIR."""
        
        processed_chunks_total = 0
        processed_files_count = 0
        header_columns = None # Read header from the first valid file

        os.makedirs(STREAMING_LOG_DIR, exist_ok=True)
        os.makedirs(ARCHIVE_DIR, exist_ok=True)

        try:
            log_files = [f for f in os.listdir(STREAMING_LOG_DIR) 
                         if os.path.isfile(os.path.join(STREAMING_LOG_DIR, f)) and f.endswith('.txt')]
        except OSError as e:
            print(f"Error listing directory {STREAMING_LOG_DIR}: {e}")
            return 0 # Cannot proceed if directory listing fails

        if not log_files:
            # print(f"No new log files found in {STREAMING_LOG_DIR}.") # Optional: Can be noisy
            return 0

        print(f"Found {len(log_files)} new log files to process.")

        for filename in log_files:
            file_path = os.path.join(STREAMING_LOG_DIR, filename)
            archive_path = os.path.join(ARCHIVE_DIR, filename)
            processed_chunks_file = 0
            
            try:
                # Read header only once from the first file encountered
                if header_columns is None:
                     header_columns = self.read_header(file_path)
                     if not header_columns:
                         print(f"Warning: Could not read header from {filename}, skipping subsequent files in this run.")
                         break # Stop processing further files if header is bad

                with open(file_path, 'r', encoding='utf-8') as f:
                    _ = f.readline() # Skip header row
                    
                    while True:
                        chunk_lines = []
                        for _ in range(chunk_size):
                            line = f.readline()
                            if not line:
                                break
                            chunk_lines.append(line)
                        
                        if not chunk_lines:
                            break 
                            
                        dataframe_chunk = self._create_dataframe_from_chunk_lines(chunk_lines, header_columns)
                        
                        if dataframe_chunk is not None and len(dataframe_chunk) > 0:
                             task_queue.put(dataframe_chunk)
                             processed_chunks_file += 1
                        elif dataframe_chunk is None:
                             print(f"Warning: Failed to create DataFrame for a chunk in {filename}. Chunk ignored.")

                # Move file to archive only after successful processing
                shutil.move(file_path, archive_path)
                print(f"Processed and archived {filename} ({processed_chunks_file} chunks).")
                processed_chunks_total += processed_chunks_file
                processed_files_count += 1

            except (IOError, OSError) as e_io:
                print(f"Error processing file {filename}: {e_io}. Skipping file.")
                # Decide if you want to leave the file or move it to an error folder
            except ValueError as e_val:
                 print(f"Error processing file {filename}: {e_val}. Skipping file.")
            except Exception as e:
                print(f"Unexpected error processing file {filename}: {e}. Skipping file.")

        print(f"Finished processing run. Processed {processed_files_count} files, {processed_chunks_total} total chunks queued.")
        return processed_chunks_total

    def extract_table_from_db_incremental(
        self,
        db_path: str,
        table_name: str,
        chunk_size: int,
        task_queue,
        marker_file: str,
        marker_column: str = None  # Se None, usará rowid
    ) -> int:
        """
        Extrai dados novos de uma tabela SQLite de forma incremental, evitando reprocessar registros já lidos.
        Se marker_column for fornecido, usa-o (ex: 'start_date'); do contrário, usa o rowid do SQLite.
        Os registros são enviados em chunks como objetos DataFrame para a task_queue.

        Args:
            db_path (str): Caminho para o arquivo .db.
            table_name (str): Nome da tabela.
            chunk_size (int): Número de linhas por chunk.
            task_queue: Fila para envio dos DataFrames.
            marker_file (str): Arquivo que guarda o último marcador processado.
            marker_column (str, opcional): Nome da coluna usada para controle incremental (ex: 'start_date').
                                            Se None, usa rowid.

        Retorna:
            int: Número total de chunks extraídos.
        """
        # Garante que a pasta do arquivo marcador existe
        marker_dir = os.path.dirname(marker_file)
        if marker_dir:  # evita erro se marker_file for só "content.txt"
            os.makedirs(marker_dir, exist_ok=True)

        if not os.path.exists(db_path):
            print(f"[extract_incremental] DB não encontrado: {db_path}")
            return 0

        # Carregar o último marcador processado
        last_processed = "0"
        if os.path.exists(marker_file):
            with open(marker_file, "r", encoding="utf-8") as f:
                last_processed = f.read().strip()
        # Se marker_column for fornecido, assumimos que é string com formato de data ou similar; 
        # caso contrário, trabalhamos com números (rowid) – por isso iniciamos em "0"
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Definir qual coluna usar
            if marker_column:
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE {marker_column} > ?
                    ORDER BY {marker_column} ASC
                """
            else:
                # Se não houver marcador definido, usamos o rowid
                query = f"""
                    SELECT rowid, * FROM {table_name}
                    WHERE rowid > ?
                    ORDER BY rowid ASC
                """
            cursor.execute(query, (last_processed,))
            # Obter nomes das colunas
            columns = [desc[0] for desc in cursor.description]
            chunk_count = 0
            max_marker_seen = last_processed

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                df_chunk = DataFrame(columns=columns)
                for row in rows:
                    # Caso usemos rowid, o primeiro campo é esse marcador
                    row_dict = dict(zip(columns, row))
                    df_chunk.add_row(list(row))
                    # Atualiza o marcador conforme o campo selecionado
                    if marker_column:
                        current_marker = row_dict.get(marker_column)
                    else:
                        current_marker = str(row_dict.get("rowid"))
                    if current_marker and current_marker > max_marker_seen:
                        max_marker_seen = current_marker

                task_queue.put(df_chunk)
                chunk_count += 1

            # Atualiza o arquivo de controle com o último marcador processado, se houve algum novo dado
            if chunk_count > 0:
                with open(marker_file, "w", encoding="utf-8") as f:
                    f.write(max_marker_seen)

            print(f"[extract_incremental] {chunk_count} chunks extraídos da tabela '{table_name}' com marcador > {last_processed}.")
            return chunk_count

        except sqlite3.Error as e:
            print(f"[extract_incremental] Erro ao acessar a tabela '{table_name}': {e}")
            return 0

        finally:
            if conn:
                conn.close()

    def read_csv_to_dataframe(self, file_path, expected_columns):
        """Reads a CSV file into a DataFrame object.
        Handles FileNotFoundError and returns an empty DataFrame with expected columns if file is missing or empty.
        Assumes the CSV has a header row matching expected_columns.
        """
        dataframe = DataFrame(columns=expected_columns)
        
        if not os.path.exists(file_path):
            print(f"Info: CSV file not found: {file_path}. Returning empty DataFrame.")
            return dataframe # Return empty DataFrame if file doesn't exist
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                header_line = f.readline().strip()
                if not header_line:
                    print(f"Warning: CSV file is empty: {file_path}. Returning empty DataFrame.")
                    return dataframe # Return empty if only header (or empty file)
                    
                # Basic header validation (optional but recommended)
                read_columns = [h.strip() for h in header_line.split(',')]
                if read_columns != expected_columns:
                    print(f"Warning: CSV header {read_columns} does not match expected {expected_columns} in {file_path}. Proceeding, but results may be inconsistent.")
                    # You might want to return dataframe here or raise an error depending on strictness

                for line in f:
                    row_line = line.strip()
                    if not row_line:
                        continue
                    row_values = [val.strip() for val in row_line.split(',')]
                    if len(row_values) == len(expected_columns):
                        try:
                            # Attempt to add row - assumes types match DataFrame internal storage
                            # You might need type conversion here depending on DataFrame.add_row implementation
                            dataframe.add_row(row_values)
                        except Exception as e_add:
                            print(f"Warning: Error adding row from CSV {file_path}: {row_values}. Error: {e_add}")
                    else:
                         print(f"Warning: Skipping row with incorrect column count in {file_path}: {row_values}")

        except FileNotFoundError:
             # This case is handled by the os.path.exists check above, but kept for robustness
             print(f"Info: CSV file not found: {file_path}. Returning empty DataFrame.")
             return DataFrame(columns=expected_columns) 
        except IOError as e:
            print(f"Error reading CSV file '{file_path}': {e}")
            # Decide: return empty DF or raise error?
            return DataFrame(columns=expected_columns)
        except Exception as e:
            print(f"Unexpected error reading CSV '{file_path}': {e}")
            return DataFrame(columns=expected_columns)
            
        return dataframe

    def read_table_to_dataframe(self, table_name: str) -> DataFrame:
        """
        Lê toda a tabela SQLite especificada e retorna como DataFrame.

        Args:
            table_name: nome da tabela no banco.

        Returns:
            DataFrame com todas as colunas e linhas da tabela.
        """
        query = f"SELECT * FROM {table_name}"
        return self.execute_query_to_dataframe(query, expected_columns=None)
    def execute_query_to_dataframe(self, query: str, expected_columns: list = None) -> DataFrame:
        """
        Executa uma query SQL e converte o resultado para DataFrame.
        Parâmetro expected_columns é ignorado, existindo para compatibilidade.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        cols = [d[0] for d in cursor.description]
        df = DataFrame(columns=cols)
        for row in cursor.fetchall():
            df.add_row(list(row))
        conn.close()
        return df

    def save_dataframe_to_csv(self, dataframe, file_path):
        if not isinstance(dataframe, DataFrame):
            raise TypeError("O argumento 'dataframe' deve ser uma instância da classe DataFrame.")

        if not file_path or not isinstance(file_path, str):
             raise ValueError("O argumento 'file_path' deve ser uma string não vazia.")

        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, 'w', encoding='utf-8', newline='') as f:
                header_line = ','.join(dataframe.columns)
                f.write(header_line + '\n')

                for row_index in range(len(dataframe)):
                    row_values = [str(dataframe[col][row_index]) for col in dataframe.columns]
                    row_line = ','.join(row_values)
                    f.write(row_line + '\n')

        except IOError as e:
            print(f"Erro de I/O ao salvar o arquivo CSV '{file_path}': {e}")
            raise
        except Exception as e:
            print(f"Erro inesperado ao salvar o DataFrame em CSV '{file_path}': {e}")
            raise

