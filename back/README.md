# BURP ES Backend

Backend em FastAPI + SQLite para ingestão e consulta de salários do Portal da Transparência federal e bolsas FAPES/FACTO/FEST no recorte do Espírito Santo.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Rodar API

```bash
uvicorn burp.api.app:app --reload --host 0.0.0.0 --port 8000
```

## Endpoints principais

- `GET /health`
- `GET /sources`
- `POST /ingest/run`
- `POST /refresh/query`
- `GET /search`
- `GET /summary`
- `GET /person/{person_key}`

## Busca com análise de teto

Exemplo:

```bash
curl "http://localhost:8000/search?nome=JOSE%20DA%20SILVA&uf=ES&tipo=todos&data_inicio=2025-01-01&data_fim=2025-12-31"
```

O retorno inclui `period_report`, com:

- soma mensal de `FOLHA` e `BOLSA`
- meses acima do teto de referência
- totais por fonte e por tipo
- cobertura do intervalo analisado

## Atualização explícita das fontes

Busca agora é somente leitura por padrão. Para atualizar Portal federal, FAPES, FACTO e FEST sob demanda:

```bash
curl -X POST http://localhost:8000/refresh/query \
  -H "Content-Type: application/json" \
  -d '{"nome":"JOSE DA SILVA","cpf":"12312312312","data_inicio":"2025-01-01","data_fim":"2025-12-31","include_fapes":true,"include_facto":true,"include_fest":true,"include_federal":true}'
```

## CLI

```bash
python -m burp ingest --target federal
python -m burp search --nome "JOSE DA SILVA" --uf ES --tipo todos --data-inicio 2025-01-01 --data-fim 2025-12-31
python -m burp sources
```

## Smoke test

O smoke test agora usa banco temporário e fixtures locais:

```bash
python scripts/smoke_test.py
```
