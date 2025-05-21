from typing import List, Tuple, Dict, Any


class DataFrame:
    """
    An implementation of a dataframe-like structure for storing tabular data.

    Attributes:
        _data (Dict[str, List[Any]]): Internal storage for the data, where keys are
            column names and values are lists of column data.
        _columns (List[str]): List of column names.
        _num_rows (int): Number of rows in the DataFrame.
    """

    def __init__(self, columns: List[str] = None) -> None:
        """
        Initializes a DataFrame with the specified columns.

        Args:
            columns (List[str], optional): A list of column names. Defaults to None.

        Raises:
            TypeError: If `columns` is not a list of strings.
        """
        self._data: Dict[str, List[Any]] = {}
        self._columns: List[str] = []
        self._num_rows: int = 0
        self._num_cols: int = 0

        if columns is not None:
            if not isinstance(columns, List) or not all(isinstance(c, str) for c in columns):
                raise TypeError('Argument `columns` must be a list of strings.')
            
            self._columns = columns
            self._num_cols = len(columns)
            for col in self._columns:
                self._data[col] = []

    def __repr__(self) -> str:
        """
        Returns a string representation of the DataFrame.

        Returns:
            str: String representation of the DataFrame.
        """
        return f"DataFrame({self._num_rows} rows, {self._num_cols} cols, columns={self.columns})"

    def __len__(self) -> int:
        """
        Returns the number of rows in the DataFrame.

        Returns:
            int: Number of rows.
        """
        return self._num_rows
        
    def __getitem__(self, key: str | int | Tuple[str, int]) -> Any:
        """
        Retrieves data from the DataFrame based on the key.

        Args:
            key (Any): The key to retrieve data. It can be:
                - A string (str): The name of the column to retrieve.
                - An integer (int) : The index of a row to retrieve.
                - A tuple (str, int): A tuple where the first element is the column name
                and the second element is the row index.

        Returns:
            Any: The data corresponding to the key.

        Raises:
            KeyError: If the column name does not exist.
            IndexError: If the row index is out of range.
            TypeError: If the key is not a string or a tuple.
        """
        if isinstance(key, str):
            if key not in self._data:
                raise KeyError(f"Column '{key}' does not exist in the DataFrame.")
            return self._data[key]
        
        elif isinstance(key, int):
            if key < 0 or key >= self._num_rows:
                raise IndexError(f"Row index {key} is out of range.")
            return {col: self._data[col][key] for col in self._columns}
        
        elif isinstance(key, Tuple) and len(key) == 2:
            column_name, row_index = key
            if column_name not in self._data:
                raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")
            if not isinstance(row_index, int) or row_index < 0 or row_index >= self._num_rows:
                raise IndexError(f"Row index {row_index} is out of range.")
            return self._data[column_name][row_index]
        
        else:
            raise TypeError("Key must be a string (column name) or a tuple (column name, row index).")

    @property
    def columns(self) -> List[str]:
        """
        Returns the list of column names.

        Returns:
            List[str]: List of column names.
        """
        return list(self._columns)

    @property
    def shape(self) -> Tuple[int, int]:
        """
        Returns the shape of the DataFrame as a tuple (number of rows, number of columns).

        Returns:
            Tuple[int, int]: Shape of the DataFrame.
        """
        num_cols: int = len(self._columns)
        return (self._num_rows, num_cols)
    def merge(self, other: 'DataFrame', on: str) -> 'DataFrame':
        """
        Realiza inner join com outro DataFrame na coluna `on`.

        Args:
            other: DataFrame para merge.
            on: nome da coluna de join (deve existir em ambos).

        Returns:
            DataFrame resultante do join.
        """
        if on not in self._columns or on not in other._columns:
            raise KeyError(f"Column '{on}' must exist in both DataFrames to merge")
        new_columns = list(self._columns) + [c for c in other._columns if c != on]
        result = DataFrame(new_columns)
        lookup: Dict[Any, List[int]] = {}
        for j in range(len(other)):
            key = other[on][j]
            lookup.setdefault(key, []).append(j)
        for i in range(len(self)):
            left_row = self[i]
            key = left_row[on]
            if key in lookup:
                for j in lookup[key]:
                    right_row = other[j]
                    row_vals = [left_row[col] for col in self._columns]
                    row_vals += [right_row[col] for col in other._columns if col != on]
                    result.add_row(row_vals)
        return result

    def head(self, n: int = 5) -> None:
        """
        Prints the first `n` rows of the DataFrame.

        Args:
            n (int, optional): Number of rows to display. Defaults to 5.
        """
        for col in self._columns:
            print(col, end='\t')
        print()
        for row in range(min(n, self._num_rows)):
            for col in self._columns:
                print(self._data[col][row], end='\t')
            print()

    def add_row(self, row_values: List[Any]) -> None:
        """
        Adds a new row to the DataFrame.

        Args:
            row_values (List[Any]): A list of values corresponding to the columns.

        Raises:
            ValueError: If the number of values does not match the number of columns.
            TypeError: If `row_values` is not a list.
        """
        if not self._columns:
            raise ValueError('Columns must be defined before adding rows.')

        if not isinstance(row_values, list):
            raise TypeError('Argument `row_values` must be a list of values.')

        if len(row_values) != len(self._columns):
            raise ValueError(
                f'Mismatched number of column values: '
                f'expected {len(self._columns)}, got {len(row_values)}.'
            )

        for i, col_name in enumerate(self._columns):
            self._data[col_name].append(row_values[i])

        self._num_rows += 1
    @classmethod
    def from_rows(cls, columns: List[str], rows: List[Tuple]) -> "DataFrame":
        """
        Create a DataFrame given column names and a list of row‐tuples.
        """
        df = cls(columns)
        for row in rows:
            # convert the tuple to a list of cell values
            df.add_row(list(row))
        return df

    def vconcat(self, other_df: "DataFrame") -> None:
        """
        Vertically concatenates another DataFrame to the current DataFrame.

        Args:
            other_df (DataFrame): The DataFrame to concatenate.
                It must have the same columns as the current DataFrame.

        Raises:
            TypeError: If `other_df` is not an instance of DataFrame.
            ValueError: If the columns of the two DataFrames do not match.
            ValueError: If a column in the current DataFrame is missing in `other_df`.

        Returns:
            None
        """
        if not isinstance(other_df, DataFrame):
            raise TypeError("Can only vertically concatenate another DataFrame object.")

        if not self._columns:
            self._columns = list(other_df.columns)
            for col in self._columns:
                if col not in self._data:
                    self._data[col] = []
        elif set(self.columns) != set(other_df.columns):
            raise ValueError("DataFrames must have the same columns to vertically concatenate.")

        if len(other_df) == 0:
            return

        num_rows_to_add: int = len(other_df)
        for col_name in self._columns:
            if col_name not in other_df._data:
                raise ValueError(f"Column '{col_name}' not found in the concatenating DataFrame.")
            self._data[col_name].extend(other_df._data[col_name])

        self._num_rows += num_rows_to_add

    def filter(self, predicate) -> "DataFrame":
        """
        Retorna um novo DataFrame contendo apenas as linhas para as quais
        `predicate(row_dict)` devolve True.

        Exemplo:
            recent = df.filter(lambda r: r["valor"] > 100)
        """
        new_df = DataFrame(self._columns.copy())
        for i in range(self._num_rows):
            row = {col: self._data[col][i] for col in self._columns}
            if predicate(row):
                new_df.add_row([row[c] for c in self._columns])
        return new_df
    def rename_column(self, old_name: str, new_name: str) -> None:
        """
        Renomeia uma coluna preservando os dados.

        Args:
            old_name: Nome atual da coluna.
            new_name: Novo nome.

        Raises:
            KeyError: Se `old_name` não existir ou `new_name` já existir.
        """
        if old_name not in self._columns:
            raise KeyError(f"Column '{old_name}' not found.")
        if new_name in self._columns:
            raise KeyError(f"Column '{new_name}' already exists.")

        # move dados
        self._data[new_name] = self._data.pop(old_name)

        # atualiza lista de colunas
        idx = self._columns.index(old_name)
        self._columns[idx] = new_name

if __name__ == '__main__':
    # Example usage of the DataFrame class
    df = DataFrame(['nome', 'idade', 'cidade'])
    for row in [
        ['João', 30, 'São Paulo'],
        ['Maria', 25, 'Rio de Janeiro'],
        ['Pedro', 35, 'Belo Horizonte'],
        ['Ana', 28, 'Curitiba'],
        ['Carlos', 40, 'Brasília'],
    ]:
        df.add_row(row)
    
    print(df)
    print('DataFrame shape:', df.shape)
    print('DataFrame length:', len(df))
    print('Column \'nome\':', df['nome'])
    print('Row #3:', df[3])
    print('Column names:', df.columns)
    df.head()
