import atoti as tt

from .opentelemetry import span
from .skeleton import Skeleton

@span
def create_trades_table(session: tt.Session, /) -> None:
    # Usamos las constantes del Skeleton
    tables = Skeleton.tables
    columns = tables.TRADES_COLUMNS

    session.create_table(
        tables.TRADES,
        data_types={
            columns.TRADE_ID: tt.STRING,
            columns.AS_OF_DATE: tt.LOCAL_DATE,
            columns.DESK: tt.STRING,
            columns.BOOK: tt.STRING,
            columns.PRODUCT_TYPE: tt.STRING,
            columns.CURRENCY: tt.STRING,
            columns.NOTIONAL: tt.DOUBLE,
            columns.MARKET_VALUE: tt.DOUBLE,
            columns.DELTA: tt.DOUBLE,
            columns.VEGA: tt.DOUBLE,
        },
        keys=[columns.TRADE_ID, columns.AS_OF_DATE],
    )

@span
def create_books_table(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    columns = tables.BOOKS_COLUMNS

    session.create_table(
        tables.BOOKS,
        data_types={
            columns.BOOK: tt.STRING,
            columns.OWNER: tt.STRING,
            columns.REGION: tt.STRING,
            columns.RISK_APPETITE: tt.STRING,
        },
        keys=[columns.BOOK],
    )

@span
def join_tables(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    trades_cols = tables.TRADES_COLUMNS
    books_cols = tables.BOOKS_COLUMNS

    # Unimos usando las referencias del esqueleto
    session.tables[tables.TRADES].join(
        session.tables[tables.BOOKS],
        session.tables[tables.TRADES][trades_cols.BOOK]
        == session.tables[tables.BOOKS][books_cols.BOOK],
    )

@span
def create_and_join_tables(session: tt.Session, /) -> None:
    create_trades_table(session)
    create_books_table(session)
    join_tables(session)
