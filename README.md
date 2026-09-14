# CDC Lakehouse

Pipeline de replicação CDC (Change Data Capture) de uma fonte transacional simulando SAP
para um lakehouse medalhão no Databricks — com SCD Tipo 1 e Tipo 2, qualidade de dados,
testes automatizados, deploy versionado e dashboards.

Projeto de portfólio e aprendizado. O plano completo, fase a fase, está em [ROADMAP.md](ROADMAP.md).

## Stack

| Camada | Ferramenta |
|---|---|
| Plataforma | Databricks Free Edition (serverless) |
| Ingestão | Auto Loader |
| Transformação | Lakeflow Declarative Pipelines (Python) |
| Governança | Unity Catalog (catálogo, schemas, Volumes) |
| Dev local | Databricks Connect + VS Code + pytest |
| Deploy | Databricks CLI + Asset Bundles |
| CI | GitHub Actions |
| Visualização | AI/BI Dashboards + Genie |

## Modelo de dados simulado

Três entidades, no espírito de SAP SD mas simplificadas:

- **customers** — muda com o tempo (cidade, segmento) → alvo do SCD Tipo 2
- **orders** — muda de status (criado → faturado → entregue → cancelado) → SCD Tipo 1
- **order_items** — itens do pedido, podem ser adicionados, alterados e removidos → delete propagation

Unity Catalog: catálogo `cdc_lakehouse`, schemas `bronze` / `silver` / `gold`, Volume
`cdc_lakehouse.bronze.raw`.

## Estrutura do repositório

```
cdc-lakehouse/
├── databricks.yml              # bundle: pipeline + job + dashboard
├── README.md
├── ROADMAP.md
├── src/
│   ├── generator/               # gerador de eventos (roda local via dbconnect)
│   ├── pipelines/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   └── transforms/              # funções puras, testáveis
├── tests/
│   ├── unit/
│   └── integration/
├── dashboards/
└── .github/workflows/
```

## Como rodar localmente

Pré-requisitos: [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html)
autenticado com um Personal Access Token do workspace (`databricks auth login` ou
`~/.databrickscfg` com um `[DEFAULT]` apontando para o workspace do Free Edition).

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv -r requirements.txt

# valida o bundle
databricks bundle validate

# smoke test: cria um DataFrame no serverless via Databricks Connect
.venv/Scripts/python.exe -m src.generator._smoke_test
```

> Databricks Connect ainda não suporta Python 3.13/3.14 — use 3.12.

## Fluxo de branches

- `main` — estável, o que está de fato validado no workspace
- `dev` — integração das features
- `feature/*` — trabalho em andamento, sempre a partir de `dev` e mergeado de volta em `dev`

## Roadmap

Veja [ROADMAP.md](ROADMAP.md) para o detalhamento fase a fase (objetivo, o que construir,
o que aprender, critério de "pronto" e armadilhas conhecidas de cada etapa).
