"""
Este script mueve los datos crudos a la RAM
"""

from pathlib import Path
import atoti as tt
import pandas as pd
from pydantic import HttpUrl

from .config import Config
from .opentelemetry import span
from .path import RESOURCES_DIRECTORY
from .skeleton import Skeleton   # Donde hemos definido nombres de tablas y columnas

# "async" indica que es una función asíncrona
@span
async def load_tables(
    session: tt.Session,
    /,
    *,
    config: Config,
    **kwargs,
) -> None:
    # 1. Rutas
    trades_path = RESOURCES_DIRECTORY / "trades.csv"
    books_path = RESOURCES_DIRECTORY / "books.csv"

    # 2. Cargamos los datos convirtiendo la columna de fecha
    # Añadimos parse_dates para que "AsOfDate" sea una fecha de verdad
    trades_df = pd.read_csv(trades_path, parse_dates=[Skeleton.tables.TRADES_COLUMNS.AS_OF_DATE])
    books_df = pd.read_csv(books_path)

    # 3. Metemos los datos en Atoti
    tables = Skeleton.tables
    with session.tables.data_transaction():
        session.tables[tables.TRADES].load(trades_df)
        session.tables[tables.BOOKS].load(books_df)
