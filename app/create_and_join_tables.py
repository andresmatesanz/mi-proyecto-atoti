import atoti as tt

from .opentelemetry import span
from .skeleton import Skeleton

# Función principal que llama a funciones que se crean en este script
@span
def create_and_join_tables(session: tt.Session, /) -> None:
    create_sensitivities_table(session)
    create_trade_info_table(session)
    create_risk_factors_table(session)
    join_tables(session)

@span
def create_sensitivities_table(session: tt.Session, /) -> None:
    # Usamos las constantes del Skeleton
    tables = Skeleton.tables
    columns = tables.SENSITIVITIES_COLUMNS

    session.create_table(
        tables.SENSITIVITIES,
        # En "data_types" se define el tipo de cada columna de la tabla "TRADES"
        data_types={
            columns.TRADE_ID: tt.STRING,
            columns.AS_OF_DATE: tt.LOCAL_DATE,
            columns.RISK_FACTOR: tt.STRING,
            columns.DELTA: tt.DOUBLE,
            columns.VEGA: tt.DOUBLE,
            columns.CURVATURE_UP: tt.DOUBLE,
            columns.CURVATURE_DOWN: tt.DOUBLE,
        },
        keys=[columns.TRADE_ID, columns.AS_OF_DATE, columns.RISK_FACTOR],
    )

@span
def create_trade_info_table(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    columns = tables.TRADE_INFO_COLUMNS

    session.create_table(
        tables.TRADE_INFO,
        data_types={
            columns.TRADE_ID: tt.STRING,
            columns.DESK: tt.STRING,
            columns.BOOK: tt.STRING,
            columns.COUNTERPARTY: tt.STRING,
            columns.PRODUCT_TYPE: tt.STRING,
        },
        keys=[columns.TRADE_ID],
    )

@span
def create_risk_factors_table(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    columns = tables.RISK_FACTORS_COLUMNS

    session.create_table(
        tables.RISK_FACTORS,
        data_types={
            columns.RISK_FACTOR: tt.STRING,
            columns.RISK_CLASS: tt.STRING,
            columns.BUCKET: tt.STRING,
            columns.CURRENCY: tt.STRING,
        },
        keys=[columns.RISK_FACTOR],
    )

@span
def join_tables(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    sensi_cols = tables.SENSITIVITIES_COLUMNS
    trade_cols = tables.TRADE_INFO_COLUMNS
    risk_cols = tables.RISK_FACTORS_COLUMNS

    # Join 1: Sensitivities.TradeId -> TradeInfo.TradeId
    # Se unirían automáticamente por TradeId pero lo hacemos de forma explícita
    session.tables[tables.SENSITIVITIES].join(
        session.tables[tables.TRADE_INFO],
        session.tables[tables.SENSITIVITIES][sensi_cols.TRADE_ID]
        == session.tables[tables.TRADE_INFO][trade_cols.TRADE_ID],
    )
    # Join 2: Sensitivities.RiskFactor -> RiskFactors.RiskFactor
    # Se unirían automáticamente por RiskFactor pero lo hacemos de forma explícita
    session.tables[tables.SENSITIVITIES].join(
        session.tables[tables.RISK_FACTORS],
        session.tables[tables.SENSITIVITIES][sensi_cols.RISK_FACTOR]
        == session.tables[tables.RISK_FACTORS][risk_cols.RISK_FACTOR],
    )
