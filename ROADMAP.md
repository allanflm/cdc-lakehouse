# CDC Lakehouse — Roadmap

Pipeline de replicação CDC de uma fonte transacional (simulando SAP) para um lakehouse
medalhão no Databricks, com SCD Tipo 2, qualidade de dados, testes automatizados,
deploy versionado e dashboards.

**Objetivo:** portfólio + aprendizado. Cada fase é fechada e demonstrável sozinha.

---

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

- **customers** — muda com o tempo (cidade, segmento) → alvo do **SCD Tipo 2**
- **orders** — muda de status (criado → faturado → entregue → cancelado) → **SCD Tipo 1**
- **order_items** — itens do pedido, podem ser adicionados, alterados e removidos → testa **delete propagation**

Toda linha emitida carrega `op` (`I`/`U`/`D`) e `updated_at`, que é o padrão de saída de
qualquer ferramenta de CDC real.

## Estrutura do repositório

```
cdc-lakehouse/
├── databricks.yml              # bundle: pipeline + job + dashboard
├── README.md
├── ROADMAP.md
├── src/
│   ├── generator/              # gerador de eventos (roda local via dbconnect)
│   ├── pipelines/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   └── transforms/             # funções puras, testáveis
├── tests/
│   ├── unit/
│   └── integration/
├── dashboards/
└── .github/workflows/
```

---

## Fase 0 — Fundação

**Construir:** repo no GitHub, Databricks CLI autenticado com PAT, `databricks bundle init`,
catálogo `cdc_lakehouse` com schemas `bronze`/`silver`/`gold`, Volume `raw`,
ambiente virtual local com `databricks-connect` e `pytest`.

**Aprender:** como o Free Edition se conecta ao mundo local; por que o OAuth falha nesse host
e o PAT resolve; o que é um bundle.

**Pronto quando:** `databricks bundle validate` passa e um script local cria um DataFrame
no serverless via Databricks Connect.

**Armadilhas:** DBFS não existe — tudo em `/Volumes/...`. Na pipeline do bundle, marque
`serverless: true`, não há job cluster pra declarar.

---

## Fase 1 — Gerador de eventos CDC

**Construir:** script Python que gera uma carga inicial (full load) e depois rodadas
incrementais com inserts, updates e deletes plausíveis — cliente muda de cidade, pedido
avança de status, item é cancelado. Grava JSON particionado por data no Volume.

**Aprender:** como um payload de CDC é formado de verdade (chave, operação, timestamp de
ordenação) — o mesmo raciocínio do replicador do trabalho.

**Pronto quando:** existem arquivos no Volume e você consegue explicar, olhando um deles,
qual registro mudou e por quê.

**Armadilhas:** o `updated_at` precisa ser monotônico por chave, senão o SCD2 vira bagunça.
Gere eventos fora de ordem de propósito depois — é exatamente o que o `sequence_by` resolve.

---

## Fase 2 — Bronze com Auto Loader

**Construir:** streaming tables lendo o Volume com Auto Loader, uma por entidade. Schema
evolution ligado, coluna de rescued data, metadados do arquivo de origem e timestamp de
ingestão. Nada de transformação aqui — Bronze é fiel à origem.

**Aprender:** por que ingestão incremental por arquivo é melhor que `spark.read` do diretório
inteiro; o que o checkpoint guarda; o que acontece quando chega uma coluna nova.

**Pronto quando:** rodar o gerador duas vezes processa só os arquivos novos.

**Armadilhas:** um full refresh apaga o histórico de processamento. Guarde isso pra quando
precisar reprocessar de propósito.

---

## Fase 3 — Silver com CDC e SCD2

**Construir:** `AUTO CDC` (ex-`APPLY CHANGES INTO`) para as três entidades:

- `customers` → **SCD Tipo 2**, com `__START_AT` / `__END_AT`
- `orders` → **SCD Tipo 1**, estado atual
- `order_items` → SCD Tipo 1 com `APPLY AS DELETE WHEN op = 'D'`

Mais expectations de qualidade: chave não nula (drop), valor negativo (fail), status fora do
domínio esperado (warn).

**Aprender:** a diferença prática entre os dois tipos de SCD, o papel do `sequence_by`, e as
três severidades de expectation. **Esta é a fase central do projeto.**

**Pronto quando:** você consulta um cliente específico e vê a linha histórica fechada e a
vigente aberta, e um item deletado na origem sumiu da Silver.

**Armadilhas:** a tabela alvo do AUTO CDC não pode ter transformação no meio do caminho —
limpe antes, numa view.

---

## Fase 4 — Gold

**Construir:** `dim_customer` (com histórico), `dim_product`, `fct_orders` e `fct_order_items`
resolvendo a chave do cliente **vigente na data do pedido** — o join histórico, que é o
motivo de existir SCD2. Mais uma agregação de receita por mês/segmento.

**Aprender:** por que um fato ligado à dimensão histórica dá resposta diferente de um ligado
ao estado atual. Se você souber explicar isso numa entrevista, já vale o projeto.

**Pronto quando:** existe uma consulta que prova a diferença entre os dois joins.

---

## Fase 5 — Orquestração e deploy versionado

**Construir:** job com tasks encadeadas (gerador → pipeline → verificação), pipeline em modo
**triggered**, tudo declarado no `databricks.yml`. GitHub Actions rodando
`bundle validate` no PR e `bundle deploy` no merge, com o PAT em secret.

**Aprender:** infra como código aplicada a dados; por que isso substitui "clicar na UI".

**Pronto quando:** um push na main atualiza a pipeline no workspace sem você abrir o browser.

**Armadilhas:** modo continuous mantém compute de pé e queima a cota diária do Free Edition.

---

## Fase 6 — Testes

**Construir:** funções de transformação isoladas em `src/transforms/` e testadas com pytest
rodando contra o serverless via Databricks Connect. Um teste de integração leve: injeta um
lote conhecido, roda a pipeline, valida o resultado esperado.

**Aprender:** como tornar código Spark testável (separar lógica de I/O).

**Pronto quando:** `pytest` roda verde no CI.

---

## Fase 7 — Dashboards

**Construir dois painéis AI/BI, versionados no bundle:**

1. **Negócio** — receita, ticket médio, funil de status, e um gráfico de mudança histórica
   que só existe por causa do SCD2.
2. **Operação** — lido do event log da pipeline
   (`event_log(TABLE(cdc_lakehouse.silver.pipeline))`): registros aprovados vs. rejeitados por
   expectation, duração por flow, lag Bronze→Gold, volume por execução.

Opcional: espaço Genie em cima da Gold.

**Aprender:** observabilidade de pipeline — o painel que diferencia o projeto de um tutorial.

---

## Fase 8 — Publicação

**Construir:** README com diagrama da arquitetura, decisões técnicas (por que SCD2 aqui e
SCD1 ali), prints dos dashboards, instruções de execução. Post no LinkedIn focado em um
aprendizado concreto, não em "concluí mais um projeto".

---

## Ordem de prioridade se o tempo apertar

Fases 0 → 3 já são um projeto defensável. A 4 e a 7 são o que fazem ele parecer profissional.
A 5 e a 6 são o que fazem ele parecer sênior.
