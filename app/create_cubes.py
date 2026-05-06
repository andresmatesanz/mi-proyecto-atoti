import atoti as tt
from .opentelemetry import span
from .skeleton import Skeleton

@span
def create_trading_cube(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    trades_cols = tables.TRADES_COLUMNS
    books_cols = tables.BOOKS_COLUMNS # Necesitaremos las columnas de Books

    cube = session.create_cube(session.tables[tables.TRADES], "Trading")
    h, l, m = cube.hierarchies, cube.levels, cube.measures

    # 1. Jerarquía de fechas
    cube.create_date_hierarchy(
        "Date parts",
        column=session.tables[tables.TRADES][trades_cols.AS_OF_DATE],
        levels={"Year": "yyyy", "Month": "MMMM", "Day": "dd"}
    )

    # 2. Jerarquía multinivel Desk -> Book
    h["Desk"] = [l["Desk"], l["Book"]]
    if "Book" in h:
        del h["Book"]

    # 3. Medidas personalizadas
    with session.data_model_transaction():
        # La medida AbsNotional
        m["AbsNotional"] = tt.math.abs(m[f"{trades_cols.NOTIONAL}.SUM"])

        # Medida que se crea de forma automática, pero si en algún momento decidimos cambiarlo a "manual" conviene crearlo
        # También si lo definimos se pueden modificar parámetros
        m["MarketValue.SUM"] = tt.agg.sum(session.tables[tables.TRADES][trades_cols.MARKET_VALUE])

@span
def create_cubes(session: tt.Session, /) -> None:
    create_trading_cube(session)
