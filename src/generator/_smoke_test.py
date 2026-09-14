"""Fase 0 — valida a conexao local -> serverless via Databricks Connect.

Uso: python -m src.generator._smoke_test
Requer DATABRICKS_CONFIG_PROFILE (ou host/token) configurado em ~/.databrickscfg.
"""

from databricks.connect import DatabricksSession


def main() -> None:
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    df = spark.range(5).withColumnRenamed("id", "n")
    df.show()
    print("OK: DataFrame criado no serverless via Databricks Connect.")


if __name__ == "__main__":
    main()
