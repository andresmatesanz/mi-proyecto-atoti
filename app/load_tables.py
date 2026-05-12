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
    # Rutas
    sensitivities_path = RESOURCES_DIRECTORY / "sensitivities.csv"
    trade_info_path = RESOURCES_DIRECTORY / "trade_info.csv"
    risk_factors_path = RESOURCES_DIRECTORY / "risk_factors.csv"

    # Cargamos los datos convirtiendo la columna de fecha
    # Usamos parse_dates para que Atoti reconozca el tt.LOCAL_DATE correctamente
    sensitivities_df = pd.read_csv(
        sensitivities_path,
        parse_dates=[Skeleton.tables.SENSITIVITIES_COLUMNS.AS_OF_DATE]
    )
    trade_info_df = pd.read_csv(trade_info_path)
    risk_factors_df = pd.read_csv(risk_factors_path)

    # Generar el calendario a partir de las fechas de sensibilidades
    unique_dates = pd.read_csv(sensitivities_path, usecols=["AsOfDate"])["AsOfDate"].unique()
    calendar_df = pd.DataFrame({"AsOfDate": pd.to_datetime(unique_dates)})

    calendar_df["Year"]    = calendar_df["AsOfDate"].dt.year.astype(str)
    calendar_df["Month"]   = calendar_df["AsOfDate"].dt.month.map(lambda mm: f"{mm:02d}")
    calendar_df["Day"]     = calendar_df["AsOfDate"].dt.day.map(lambda dd: f"{dd:02d}")
    calendar_df["Quarter"] = calendar_df["AsOfDate"].dt.quarter.map(lambda qq: f"Q{qq}")

    # IMPORTANTE: Convertir a date para Atoti LOCAL_DATE
    calendar_df["AsOfDate"] = calendar_df["AsOfDate"].dt.date

    # Metemos los datos en Atoti
    tables = Skeleton.tables
    with session.tables.data_transaction():
        session.tables[tables.SENSITIVITIES].load(sensitivities_df)
        session.tables[tables.TRADE_INFO].load(trade_info_df)
        session.tables[tables.RISK_FACTORS].load(risk_factors_df)
        session.tables["Calendar"].load(calendar_df)
