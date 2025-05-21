from DataFrame import DataFrame
from collections import defaultdict
from datetime import datetime
from typing import Dict, Tuple

class RevenueAnalyzer:
    def __init__(self, df):
        """
        Inicializa o objeto RevenueAnalyzer com o DataFrame de dados.

        Args:
            df (DataFrame): O DataFrame contendo as informações de data e valor.
        """
        self.df = df

    def _parse_date(self, date_str: str) -> datetime:
        """
        Converte uma string de data no formato YYYY-MM-DD para um objeto datetime.

        Args:
            date_str (str): A string de data no formato YYYY-MM-DD.

        Returns:
            datetime: O objeto datetime correspondente.
        """
        return datetime.strptime(date_str, "%Y-%m-%d")
    
    def _analyze_revenue(self, time_format: str) -> DataFrame:
        """
        Método genérico para análise de faturamento por diferentes intervalos de tempo (dia, mês, ano).

        Args:
            time_format (str): O formato da chave temporal ('%Y-%m-%d' para dia, '%Y-%m' para mês, '%Y' para ano).

        Returns:
            Dict[str, float]: Um dicionário com a soma dos valores para cada período de tempo.
        """
        revenue = defaultdict(float)

        for i in range(len(self.df)):
            date_str = self.df["date"][i]
            value = float(self.df["value"][i])
            date = self._parse_date(date_str)
            key = date.strftime(time_format)
            revenue[key] += value

        return dict(revenue)

    def analyze_revenue_by_day(self) -> DataFrame:
        """
        Retorna um dicionário com o faturamento total por dia.

        Returns:
            Dict[str, float]: Faturamento total por dia (formato: YYYY-MM-DD).
        """
        return self._analyze_revenue('%Y-%m-%d')

    def analyze_revenue_by_month(self) -> Dict[str, float]:
        """
        Retorna um dicionário com o faturamento total por mês (formato: YYYY-MM).

        Returns:
            Dict[str, float]: Faturamento total por mês (formato: YYYY-MM).
        """
        return self._analyze_revenue('%Y-%m')


    def analyze_revenue_by_year(self) -> DataFrame:
        """
        Retorna um dicionário com o faturamento total por ano (formato: YYYY).

        Returns:
            Dict[str, float]: Faturamento total por ano (formato: YYYY).
        """
        return self._analyze_revenue('%Y')