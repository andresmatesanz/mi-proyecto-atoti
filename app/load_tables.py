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
    sensitivities_path = RESOURCES_DIRECTORY / "sensitivities.csv"
    trade_info_path = RESOURCES_DIRECTORY / "trade_info.csv"
    risk_factors_path = RESOURCES_DIRECTORY / "risk_factors.csv"

    # 2. Cargamos los datos convirtiendo la columna de fecha
    # Añadimos parse_dates para que "AsOfDate" sea una fecha de verdad
    sensitivities_df = pd.read_csv(sensitivities_path)
    trade_info_df = pd.read_csv(trade_info_path)
    risk_factors_df = pd.read_csv(risk_factors_path)

    # 3. Metemos los datos en Atoti
    tables = Skeleton.tables
    with session.tables.data_transaction():
        session.tables[tables.SENSITIVITIES].load(sensitivities_df)
        session.tables[tables.TRADE_INFO].load(trade_info_df)
        session.tables[tables.RISK_FACTORS].load(risk_factors_df)
