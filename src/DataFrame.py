from typing import List, Tuple, Dict, Any, Optional

class DataFrame:
    """
    An implementation of a dataframe-like structure for storing tabular data.

    Attributes:
        _data (Dict[str, List[Any]]): Internal dictionary to store column data.
            Keys are column names (str), and values are lists containing the
            data for each row in that column.
        _columns (List[str]): An ordered list of column names, maintaining the
            insertion order of columns.
        _num_rows (int): The total number of rows currently in the DataFrame.
        _num_cols (int): The total number of columns currently in the DataFrame.
    """

    def __init__(self, columns: Optional[List[str]] = None) -> None:
        """
        Initializes a DataFrame with the specified columns.

        Args:
            columns (Optional[List[str]]): A list of column names to initialize
                the DataFrame. If None, an empty DataFrame is created, and columns
                can be added later via `vconcat` or by adding rows after defining
                columns. Defaults to None.

        Raises:
            TypeError: If `columns` is provided but is not a list of strings.
        """
        self._data: Dict[str, List[Any]] = {}
        self._columns: List[str] = []
        self._num_rows: int = 0
        self._num_cols: int = 0

        if columns is not None:
            if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
                raise TypeError('Argument `columns` must be a list of strings.')
            
            self._columns = columns
            self._num_cols = len(columns)
            for col in self._columns:
                self._data[col] = []

    def __repr__(self) -> str:
        """
        Returns a concise string representation of the DataFrame, showing its
        dimensions and column names.

        Returns:
            str: A string describing the DataFrame's current state.
        """
        return f"DataFrame({self._num_rows} rows, {self._num_cols} cols, columns={self.columns})"

    def __len__(self) -> int:
        """
        Returns the number of rows in the DataFrame.

        Returns:
            int: The total number of rows.
        """
        return self._num_rows
        
    def __getitem__(self, key: str | int | Tuple[str, int]) -> Any:
        """
        Retrieves data from the DataFrame using various key types.

        This method supports flexible data access:
        - By **column name**: `df['column_name']` returns a list of all values in that column.
        - By **row index**: `df[row_index]` returns a dictionary representing the row,
          where keys are column names and values are the cell data for that row.
        - By **specific cell**: `df['column_name', row_index]` returns the single
          value at the intersection of the specified column and row.

        Args:
            key (str | int | Tuple[str, int]): The key to specify the data to retrieve.
                - If a **string**, it represents a column name.
                - If an **integer**, it represents a row index.
                - If a **tuple** `(column_name, row_index)`, it specifies a single cell.

        Returns:
            Any: The retrieved data, which can be a list (for a column), a dictionary
                 (for a row), or a single value (for a cell).

        Raises:
            KeyError: If a specified column name does not exist.
            IndexError: If a specified row index is out of bounds.
            TypeError: If the key is not a string, an integer, or a valid tuple.
        """
        if isinstance(key, str):
            if key not in self._data:
                raise KeyError(f"Column '{key}' does not exist in the DataFrame.")
            return self._data[key]
        
        elif isinstance(key, int):
            if key < 0 or key >= self._num_rows:
                raise IndexError(f"Row index {key} is out of range. DataFrame has {self._num_rows} rows.")
            return {col: self._data[col][key] for col in self._columns}
        
        elif isinstance(key, Tuple) and len(key) == 2:
            column_name, row_index = key
            if column_name not in self._data:
                raise KeyError(f"Column '{column_name}' does not exist in the DataFrame.")
            if not isinstance(row_index, int) or row_index < 0 or row_index >= self._num_rows:
                raise IndexError(f"Row index {row_index} is out of range. DataFrame has {self._num_rows} rows.")
            return self._data[column_name][row_index]
        
        else:
            raise TypeError("Key must be a string (column name), an integer (row index), or a tuple (column name, row index).")

    @property
    def columns(self) -> List[str]:
        """
        Returns a new list containing the names of all columns in the DataFrame.

        Returns:
            List[str]: A copy of the list of column names.
        """
        return list(self._columns)

    @property
    def shape(self) -> Tuple[int, int]:
        """
        Returns the dimensions of the DataFrame as a tuple: (number of rows, number of columns).

        Returns:
            Tuple[int, int]: A tuple representing the (rows, columns) of the DataFrame.
        """
        return (self._num_rows, self._num_cols)
    
    def merge(self, other: 'DataFrame', on: str) -> 'DataFrame':
        """
        Performs an inner join operation with another DataFrame on a specified common column.

        This method creates a new DataFrame containing rows where the value in the
        `on` column matches in both DataFrames. The resulting DataFrame will include
        all columns from both original DataFrames, with duplicate columns (other than
        the `on` column) from the `other` DataFrame being excluded.

        Args:
            other (DataFrame): The DataFrame to merge with the current DataFrame.
            on (str): The name of the column to join on. This column must exist
                      in both DataFrames.

        Returns:
            DataFrame: A new DataFrame resulting from the inner join.

        Raises:
            KeyError: If the `on` column does not exist in either the current
                      DataFrame or the `other` DataFrame.
        """
        if on not in self._columns or on not in other._columns:
            raise KeyError(f"Column '{on}' must exist in both DataFrames to merge.")
        
        # Determine the new set of columns for the merged DataFrame
        new_columns = list(self._columns) + [c for c in other._columns if c != on]
        result_df = DataFrame(new_columns)

        # Build a lookup table for efficient matching in the `other` DataFrame
        # Key: value in the 'on' column, Value: list of row indices in 'other' DataFrame
        lookup: Dict[Any, List[int]] = {}
        for j in range(len(other)):
            key = other[on][j]
            lookup.setdefault(key, []).append(j)
        for i in range(len(self)):
            left_row_dict = self[i]
            key = left_row_dict[on]

            if key in lookup:
                for j in lookup[key]:
                    right_row_dict = other[j]
                    row_values = [left_row_dict[col] for col in self._columns]
                    row_values.extend([right_row_dict[col] for col in other._columns if col != on])
                    
                    result_df.add_row(row_values)
        return result_df

    def head(self, n: int = 5) -> None:
        """
        Prints the first `n` rows of the DataFrame to the console.

        Args:
            n (int): The number of rows to display from the top of the DataFrame.
                     Defaults to 5.
        """
        if not self._columns:
            print("DataFrame is empty (no columns defined).")
            return

        print('\t'.join(self._columns))
        print("-" * (8 * len(self._columns))) 

        # Print data rows, up to 'n' or the total number of rows if less than 'n'
        for row_index in range(min(n, self._num_rows)):
            row_data = [str(self._data[col][row_index]) for col in self._columns]
            print('\t'.join(row_data))

    def add_row(self, row_values: List[Any]) -> None:
        """
        Adds a new row of data to the DataFrame.

        Each value in `row_values` corresponds to a column in the DataFrame,
        in the order defined by `self._columns`.

        Args:
            row_values (List[Any]): A list of values to be added as a new row.
                                    The length of this list must match the
                                    number of columns in the DataFrame.

        Raises:
            ValueError: If columns have not been defined for the DataFrame,
                        or if the number of values in `row_values` does not
                        match the number of existing columns.
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
    def from_rows(cls, columns: List[str], rows: List[Tuple]) -> 'DataFrame':
        """
        Creates a new DataFrame instance populated with data from a list of rows.

        Args:
            columns (List[str]): A list of string names for the columns of the DataFrame.
            rows (List[Tuple]): A list of tuples, where each tuple represents a row
                                and its elements correspond to the values for each column.

        Returns:
            DataFrame: A new DataFrame instance populated with the provided data.

        Raises:
            TypeError: If `columns` is not a list of strings or `rows` is not a list of tuples.
            ValueError: If any row tuple's length does not match the number of columns.
        """
        if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
            raise TypeError('Argument `columns` must be a list of strings.')
        if not isinstance(rows, list) or not all(isinstance(r, tuple) for r in rows):
            raise TypeError('Argument `rows` must be a list of tuples.')
        
        df = cls(columns)
        for i, row in enumerate(rows):
            if len(row) != len(columns):
                raise ValueError(f"Row {i} has {len(row)} values, but expected {len(columns)} based on columns: {columns}.")
            # Convert the tuple to a list of cell values before adding
            df.add_row(list(row))
        return df

    def vconcat(self, other_df: 'DataFrame') -> None:
        """
        Vertically concatenates another DataFrame to the current DataFrame.

        Args:
            other_df (DataFrame): The DataFrame whose rows will be appended to
                                  the current DataFrame.

        Raises:
            TypeError: If `other_df` is not an instance of `DataFrame`.
            ValueError: If the columns of the two DataFrames do not match.
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
                raise ValueError(f"Column '{col_name}' not found in the concatenating DataFrame. All columns must match.")
            self._data[col_name].extend(other_df._data[col_name])

        self._num_rows += num_rows_to_add

    def filter(self, predicate) -> "DataFrame":
        """
        Returns a new DataFrame containing only the rows for which the `predicate`
        function returns True.

        The `predicate` function should accept a dictionary representing a row
        (where keys are column names and values are cell data) and return a boolean.

        Example:
            To get rows where the 'value' column is greater than 100:
            `recent_data = df.filter(lambda r: r["value"] > 100)`

        Args:
            predicate (Callable[[Dict[str, Any]], bool]): A function that takes
                a row (as a dictionary) and returns True if the row should be
                included in the new DataFrame, False otherwise.

        Returns:
            DataFrame: A new DataFrame containing only the rows that satisfy the predicate.
        """
        new_df = DataFrame(self._columns.copy())
        for i in range(self._num_rows):
            row_dict = {col: self._data[col][i] for col in self._columns}
            if predicate(row_dict):
                new_df.add_row([row_dict[c] for c in self._columns])
        return new_df
    
    def rename_column(self, old_name: str, new_name: str) -> None:
        """
        Renames an existing column in the DataFrame while preserving its data.

        Args:
            old_name (str): The current name of the column to be renamed.
            new_name (str): The new name for the column.

        Raises:
            KeyError: If `old_name` does not exist in the DataFrame's columns,
                      or if `new_name` already exists, which would cause a conflict.
            TypeError: If `old_name` or `new_name` are not strings.
        """
        if not isinstance(old_name, str) or not isinstance(new_name, str):
            raise TypeError("Old and new column names must be strings.")

        if old_name not in self._columns:
            raise KeyError(f"Column '{old_name}' not found in the DataFrame.")
        if new_name in self._columns:
            raise KeyError(f"Column '{new_name}' already exists in the DataFrame.")

        # Transfer the data from the old column name to the new column name
        self._data[new_name] = self._data.pop(old_name)

        # Update the list of column names to reflect the rename, maintaining order
        try:
            idx = self._columns.index(old_name)
            self._columns[idx] = new_name
        except ValueError: pass


if __name__ == '__main__':
    # Initialize a DataFrame with specific columns
    print("--- Initializing and Adding Rows ---")
    df = DataFrame(['name', 'age', 'city'])
    for row in [
        ['John', 30, 'São Paulo'],
        ['Maria', 25, 'Rio de Janeiro'],
        ['Peter', 35, 'Belo Horizonte'],
        ['Anna', 28, 'São Paulo'],
        ['Charles', 40, 'Rio de Janeiro'],
    ]:
        df.add_row(row)
    
    print(df)
    print(f'DataFrame shape: {df.shape}')
    print(f'DataFrame length: {len(df)}')
    print(f'Column \'name\': {df["name"]}')
    print(f'Row #3 (index 3): {df[3]}')
    print(f'Cell at (\'city\', 1): {df["city", 1]}')
    print(f'Column names: {df.columns}')
    print("\nFirst 3 rows:")
    df.head(3)

    print("\n--- Testing from_rows Class Method ---")
    df_from_rows = DataFrame.from_rows(
        columns=['product', 'price', 'quantity'],
        rows=[
            ('Laptop', 1200.00, 2),
            ('Mouse', 25.50, 5),
            ('Keyboard', 75.00, 3)
        ]
    )
    print(df_from_rows)
    df_from_rows.head()

    print("\n--- Testing Column Renaming ---")
    df.rename_column('name', 'first_name')
    print(df)
    df.head()

    print("\n--- Testing Vertical Concatenation (vconcat) ---")
    df2 = DataFrame(['first_name', 'age', 'city'])
    df2.add_row(['Lucy', 22, 'Salvador'])
    df2.add_row(['Mark', 38, 'Recife'])
    print("\nOriginal df (after rename):")
    df.head()
    print("\nDataFrame to concatenate (df2):")
    df2.head()

    df.vconcat(df2)
    print("\nDataFrame after vconcat:")
    df.head(10) # Display more rows to see concatenated data

    print("\n--- Testing Filtering ---")
    young_people_df = df.filter(lambda r: r["age"] < 30)
    print("\nDataFrame filtered for age < 30:")
    young_people_df.head()

    print("\n--- Testing Merge (Inner Join) ---")
    # Create another DataFrame for merging
    cities_df = DataFrame(['city', 'population'])
    cities_data = [
        ['São Paulo', 12345000],
        ['Rio de Janeiro', 6700000],
        ['Belo Horizonte', 2500000],
        ['Curitiba', 1900000],
    ]
    for row in cities_data:
        cities_df.add_row(row)

    print("\nOriginal df:")
    df.head()
    print("\nCities df:")
    cities_df.head()

    merged_df = df.merge(cities_df, on='city')
    print("\nMerged DataFrame (df merged with cities_df on 'city'):")
    merged_df.head(10)