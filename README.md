# BURP ES

Aplicação focada no recorte do Espírito Santo para cruzar salários do Portal da Transparência federal com bolsas FAPES e FACTO, com ETL em Python no `back/` e uma interface React no `front/`.

## Estrutura

- `back/`: ETL, SQLite, API FastAPI e CLI.
- `front/`: interface React para consulta e análise mensal do teto.

## Rodar o backend

```bash
cd back
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn burp.api.app:app --reload --host 0.0.0.0 --port 8000
```

## Rodar o frontend

```bash
cd front
npm install
npm run dev
```

Por padrão o frontend fala com `http://localhost:8000`. Se precisar mudar, configure `VITE_API_BASE_URL`.

## Fluxo principal

1. Ingestão para formato canônico `records`.
2. Persistência idempotente em SQLite.
3. Busca por nome com agrupamento conservador.
4. Relatório mensal por período, somando `FOLHA + BOLSA` e marcando meses acima do teto de referência.
