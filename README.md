# BURP ES - Buscador Universal de Recebimentos Publicos (Protótipo)

Protótipo funcional para buscar e consolidar recebimentos publicos (folha e bolsas) no ES, com auditabilidade e desambiguacao conservadora.

## Requisitos
- Python 3.10+
- pip (ou uv/poetry)

## Setup rapido
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Opcional:
```bash
cp .env.example .env
```

## Rodar API
```bash
uvicorn burp.api.app:app --reload --host 0.0.0.0 --port 8000
```

### Endpoints
- `GET /health`
- `GET /sources`
- `POST /ingest/run`
- `GET /search?nome=...&uf=ES&municipio=...&tipo=folha|bolsa|diaria|todos`
- `GET /person/{person_key}`

### Tipos de recebimento
- `FOLHA`: remuneracao de servidores.
- `BOLSA`: bolsas academicas/pesquisa/auxilios (quando identificado como tal).
- `DIARIA`: diaria/ajuda de custo/itens de viagem quando explicitamente identificados no dado bruto.

Exemplo:
```bash
curl -X POST http://localhost:8000/ingest/run -H "Content-Type: application/json" -d '{"targets":["vitoria","vilavelha","ckan","fapes"]}'
curl "http://localhost:8000/search?nome=JOSE%20DA%20SILVA&uf=ES&tipo=todos&rebusca=true"
curl "http://localhost:8000/search?nome=JOSE%20DA%20SILVA&uf=TODOS&tipo=todos&rebusca=true&cpf=00000000000"
```
O `/search` retorna `summary.monthly_total_avg` com a media mensal calculada a partir das competencias encontradas.
Tambem inclui `summary.total_value_by_source` e `summary.facto_total_value`.
Cada cluster traz `top_records_by_source` para ver os registros por fonte.
Para incluir fontes federais, use `uf=TODOS` na busca.
Quando `rebusca=true`, o sistema consulta o Portal Federal por nome (pessoa fisica) e inclui despesas por favorecido; para remuneracao, requer `BURP_FEDERAL_API_KEY`.

FACTO sob demanda:
```bash
curl -X POST http://localhost:8000/ingest/run -H "Content-Type: application/json" -d '{"targets":["facto"],"facto_nome":"MARIA"}'
```

## CLI
```bash
python -m burp ingest --target all
python -m burp search --nome "JOSE DA SILVA" --tipo todos --uf ES --municipio "Vitoria"
python -m burp sources
```

FACTO sob demanda:
```bash
python -m burp ingest --target facto --facto-nome "MARIA"
```

Portal Federal (configure a chave antes):
```bash
export BURP_FEDERAL_API_KEY="SUA_CHAVE_AQUI"
python -m burp ingest --target federal
```
Opcional (consulta direcionada):
```bash
export BURP_FEDERAL_CPFS="00000000000,11111111111"
export BURP_FEDERAL_IDS="12345,67890"
```

## Smoke test
Executa ingestao minima, escolhe nomes reais do banco e chama `/search`.
```bash
python scripts/smoke_test.py
```
Saida: `artifacts/smoke_results.json`

## Limitacoes
- Resolucao de entidades conservadora (cluster por nome + municipio/orgao).
- Cobertura limitada a fontes ES configuradas e uma competencia/ano por fonte.
- FAPES pode ter somente ano (sem competencia mensal).
- FACTO roda apenas sob demanda (requer nome), por padrão com janela de 30 dias (configurável em `.env`).
- Classificacao entre folha e bolsa e heuristica (conservadora).
- Portal Federal usa dados publicos de favorecidos por nome e requer API key em `BURP_FEDERAL_API_KEY` para remuneracao (pode precisar de CPF/ID valido).

## Classificacao BOLSA vs DIARIA
Reclassificacao BOLSA -> DIARIA e conservadora e so ocorre quando termos explicitos de diaria/ajuda de custo
aparecem em campos como rubrica/elemento/natureza/descricao/historico/observacao. Palavras-chave e campos podem
ser ajustados via `BURP_DIARIA_KEYWORDS`, `BURP_BOLSA_KEYWORDS`, `BURP_DIARIA_PRIMARY_FIELDS` e
`BURP_DIARIA_STRONG_FIELDS`. Para dados aninhados (ex.: `detalhes_json.raw.elemento`) use
`BURP_DIARIA_PRIMARY_JSONPATHS` e `BURP_DIARIA_STRONG_JSONPATHS`.

Backfill (registros ja persistidos):
```bash
python -m burp backfill-diaria
```

## Como adicionar novas fontes
1. Criar conector em `burp/connectors/`.
2. Adicionar metadados em `burp/connectors/sources.py`.
3. Mapear campos para `records` (modelo canonico).
4. Atualizar `burp/ingest.py` para registrar a fonte.

## Backlog (nao implementado)
- Cachoeiro de Itapemirim (prefeitura) com fallback quando portal estiver disponivel.
- Classificador de ocupacao (educacao).
- UI melhor (Next.js) com filtros e paginacao.

## Docker (opcional)
```bash
docker-compose up --build
```
