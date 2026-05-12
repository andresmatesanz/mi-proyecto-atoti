import atoti as tt
from .opentelemetry import span
from .skeleton import Skeleton

# Función principal que llama a funciones que se crean en este script
@span
def create_cubes(session: tt.Session, /) -> None:
    create_sensitivities_cube(session)

@span
def create_sensitivities_cube(session: tt.Session, /) -> None:
    tables = Skeleton.tables
    sensi_cols = tables.SENSITIVITIES_COLUMNS
    trade_cols = tables.TRADE_INFO_COLUMNS
    risk_cols = tables.RISK_FACTORS_COLUMNS

    # Definimos las referencias a las tablas de la sesión
    sensitivities = session.tables[tables.SENSITIVITIES]
    trade_info = session.tables[tables.TRADE_INFO]
    risk_factors = session.tables[tables.RISK_FACTORS]

    cube = session.create_cube(sensitivities, mode="manual", name="SensitivityCube")
    h, l, m = cube.hierarchies, cube.levels, cube.measures

    # Jerarquía de fechas
    cube.create_date_hierarchy(
        "Date parts",
        column=sensitivities[sensi_cols.AS_OF_DATE],
        levels={"Year": "yyyy", "Month": "MMMM", "Day": "dd"}
    )

    h["AsOfDate"] = [sensitivities[sensi_cols.AS_OF_DATE]]
    h["AsOfDate"].slicing = True

    # Jerarquías multinivel
    h["Org"] = {
        "Desk": trade_info["Desk"],
        "Book": trade_info["Book"],
    }

    h["RiskHierarchy"] = [
        risk_factors[risk_cols.RISK_CLASS],
        risk_factors[risk_cols.BUCKET],
        risk_factors[risk_cols.RISK_FACTOR],
    ]

    # Jerarquías Simples
    h["Counterparty"] = [trade_info[trade_cols.COUNTERPARTY]]
    h["ProductType"]  = [trade_info[trade_cols.PRODUCT_TYPE]]
    h["Currency"]     = [risk_factors[risk_cols.CURRENCY]]

    # Medidas explícitas
    with session.data_model_transaction():
        m["Delta.SUM"]         = tt.agg.sum(sensitivities["Delta"])
        m["Vega.SUM"]          = tt.agg.sum(sensitivities["Vega"])
        m["CurvatureUp.SUM"]   = tt.agg.sum(sensitivities["CurvatureUp"])
        m["CurvatureDown.SUM"] = tt.agg.sum(sensitivities["CurvatureDown"])
