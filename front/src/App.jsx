import { useEffect, useMemo, useState } from "react";

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "http://localhost:8000").replace(/\/$/, "");

function currency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(Number(value || 0));
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function startOfYearIso() {
  const now = new Date();
  return `${now.getFullYear()}-01-01`;
}

function buildQuery(params) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  });
  return query.toString();
}

async function fetchJson(path, options) {
  const response = await fetch(`${API_BASE}${path}`, options);
  const payload = await response.json();
  if (!response.ok) {
    const detail = payload?.detail || "Erro inesperado";
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return payload;
}

function SummaryCard({ label, value, helper, tone = "default" }) {
  return (
    <article className={`summary-card summary-card--${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{helper}</small>
    </article>
  );
}

function SourceBadge({ source }) {
  const status = source.last_status || "unknown";
  return (
    <article className={`source-pill source-pill--${status}`}>
      <header>
        <strong>{source.name}</strong>
        <span>{status}</span>
      </header>
      <small>{source.source_id}</small>
      <small>{source.last_error || source.last_run_at || "Sem execução registrada"}</small>
    </article>
  );
}

function MonthlyRow({ month }) {
  const entries = Object.entries(month.totals_by_source || {});
  return (
    <article className={`month-row ${month.estourou_teto ? "month-row--alert" : ""}`}>
      <div className="month-row__main">
        <div>
          <h3>{month.month}</h3>
          <p>
            Total do mês: <strong>{currency(month.total)}</strong>
          </p>
        </div>
        <div className="month-row__flag">
          <span>{month.estourou_teto ? "Acima do teto" : "Dentro do teto"}</span>
          <strong>{currency(month.teto_constitucional)}</strong>
        </div>
      </div>

      <div className="month-row__grid">
        <div>
          <span>Por tipo</span>
          <ul>
            {Object.entries(month.totals_by_tipo || {}).map(([tipo, total]) => (
              <li key={tipo}>
                <strong>{tipo}</strong>
                <span>{currency(total)}</span>
              </li>
            ))}
          </ul>
        </div>

        <div>
          <span>Por fonte</span>
          <ul>
            {entries.length ? (
              entries.map(([source, total]) => (
                <li key={source}>
                  <strong>{source}</strong>
                  <span>{currency(total)}</span>
                </li>
              ))
            ) : (
              <li>
                <strong>Sem registros</strong>
                <span>{currency(0)}</span>
              </li>
            )}
          </ul>
        </div>
      </div>

      {month.estourou_teto ? (
        <p className="month-row__alert">Excesso estimado: {currency(month.excesso)}</p>
      ) : null}
    </article>
  );
}

export default function App() {
  const [form, setForm] = useState({
    nome: "",
    municipio: "",
    uf: "ES",
    tipo: "todos",
    cpf: "",
    dataInicio: startOfYearIso(),
    dataFim: todayIso(),
  });
  const [sources, setSources] = useState([]);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [refreshInfo, setRefreshInfo] = useState(null);

  useEffect(() => {
    fetchJson("/sources")
      .then((payload) => setSources(payload.sources || []))
      .catch((err) => setError(err.message));
  }, []);

  const summary = result?.period_report?.totals;
  const homonymRisk = result?.match_context?.homonym_risk || "N/A";
  const shouldRefreshFederal = true;

  const sourceHighlights = useMemo(() => {
    if (!summary?.by_source) {
      return [];
    }
    return Object.entries(summary.by_source).slice(0, 4);
  }, [summary]);

  async function runSearch() {
    setLoading(true);
    setError("");
    try {
      const query = buildQuery({
        nome: form.nome,
        municipio: form.municipio || null,
        uf: form.uf,
        tipo: form.tipo,
        cpf: form.cpf || null,
        data_inicio: form.dataInicio || null,
        data_fim: form.dataFim || null,
      });
      const payload = await fetchJson(`/search?${query}`);
      setResult(payload);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function refreshSources() {
    setRefreshing(true);
    setError("");
    try {
      const payload = await fetchJson("/refresh/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          nome: form.nome,
          cpf: form.cpf || null,
          data_inicio: form.dataInicio || null,
          data_fim: form.dataFim || null,
          include_fapes: true,
          include_facto: true,
          include_federal: shouldRefreshFederal,
        }),
      });
      setRefreshInfo(payload.refresh);
      await runSearch();
      const sourcePayload = await fetchJson("/sources");
      setSources(sourcePayload.sources || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setRefreshing(false);
    }
  }

  function updateField(event) {
    const { name, value } = event.target;
    setForm((current) => ({ ...current, [name]: value }));
  }

  return (
    <div className="shell">
      <div className="backdrop" />

      <main className="layout">
        <section className="hero">
          <div>
            <span className="eyebrow">BURP ES</span>
            <h1>Auditoria de teto com salário e bolsas no Espírito Santo.</h1>
            <p>
              A busca consolida salários do Portal da Transparência federal com bolsas da FAPES e da FACTO e destaca,
              mês a mês, quando a soma passa do teto de referência configurado no backend.
            </p>
          </div>

          <div className="hero-card">
            <span>Recorte operacional</span>
            <strong>Espírito Santo</strong>
            <small>Portal da Transparência federal para salário + FAPES e FACTO para bolsas.</small>
          </div>
        </section>

        <section className="panel search-panel">
          <div className="panel__header">
            <div>
              <span className="eyebrow">Consulta</span>
              <h2>Investigar servidor</h2>
            </div>
            <p>Use a base local para consulta normal e atualize Portal, FAPES e FACTO quando precisar trazer dados novos.</p>
          </div>

          <div className="form-grid">
            <label>
              Nome
              <input name="nome" value={form.nome} onChange={updateField} placeholder="Nome completo" />
            </label>

            <label>
              Município
              <input name="municipio" value={form.municipio} onChange={updateField} placeholder="Opcional" />
            </label>

            <label>
              UF
              <select name="uf" value={form.uf} onChange={updateField}>
                <option value="ES">ES</option>
                <option value="TODOS">TODOS</option>
              </select>
            </label>

            <label>
              Tipo
              <select name="tipo" value={form.tipo} onChange={updateField}>
                <option value="todos">Todos</option>
                <option value="folha">Folha</option>
                <option value="bolsa">Bolsa</option>
              </select>
            </label>

            <label>
              Data inicial
              <input type="date" name="dataInicio" value={form.dataInicio} onChange={updateField} />
            </label>

            <label>
              Data final
              <input type="date" name="dataFim" value={form.dataFim} onChange={updateField} />
            </label>

            <label className="form-grid__wide">
              CPF
              <input
                name="cpf"
                value={form.cpf}
                onChange={updateField}
                placeholder="Opcional. Ajuda a refinar a busca de salário no Portal da Transparência."
              />
            </label>

          </div>

          <div className="actions">
            <button className="button button--primary" disabled={!form.nome || loading} onClick={runSearch}>
              {loading ? "Consultando..." : "Consultar base local"}
            </button>
            <button className="button button--secondary" disabled={!form.nome || refreshing} onClick={refreshSources}>
              {refreshing ? "Atualizando..." : "Atualizar Portal + FAPES + FACTO"}
            </button>
          </div>

          {refreshInfo ? (
            <div className="inline-note">
              <strong>Refresh executado.</strong>
              <span>
                Período: {refreshInfo.period?.start || "padrão"} até {refreshInfo.period?.end || "padrão"}.
              </span>
            </div>
          ) : null}

          {error ? <div className="error-box">{error}</div> : null}
        </section>

        <section className="panel">
          <div className="panel__header">
            <div>
              <span className="eyebrow">Fontes</span>
              <h2>Status operacional</h2>
            </div>
            <p>Aqui fica visível o que está saudável, o que falhou e o que ainda precisa revisão de conector.</p>
          </div>

          <div className="sources-grid">
            {sources.map((source) => (
              <SourceBadge key={source.source_id} source={source} />
            ))}
          </div>
        </section>

        {result ? (
          <>
            <section className="summary-grid">
              <SummaryCard
                label="Total no período"
                value={currency(summary?.overall)}
                helper="Soma de salários e bolsas consideradas no intervalo."
                tone="dark"
              />
              <SummaryCard
                label="Meses acima do teto"
                value={String(summary?.months_over_ceiling || 0)}
                helper="Meses em que o total estimado passou do teto de referência."
                tone={summary?.months_over_ceiling ? "alert" : "good"}
              />
              <SummaryCard
                label="Maior mês"
                value={summary?.max_month || "Sem dado"}
                helper={currency(summary?.max_month_total || 0)}
              />
              <SummaryCard
                label="Risco de homônimo"
                value={homonymRisk}
                helper="Quanto maior o risco, mais importante revisar os agrupamentos antes de concluir."
                tone={homonymRisk === "HIGH" ? "alert" : homonymRisk === "MEDIUM" ? "warm" : "good"}
              />
            </section>

            <section className="panel">
              <div className="panel__header">
                <div>
                  <span className="eyebrow">Quebra Mensal</span>
                  <h2>Teto constitucional por mês</h2>
                </div>
                <p>
                  Referência usada: {result.period_report?.ceiling_reference?.source}. Valores posteriores ao último
                  ano configurado reaproveitam o teto conhecido até ajuste no backend.
                </p>
              </div>

              <div className="month-list">
                {(result.period_report?.monthly || []).map((month) => (
                  <MonthlyRow key={month.month} month={month} />
                ))}
              </div>
            </section>

            <section className="panel two-column">
              <div>
                <div className="panel__header">
                  <div>
                    <span className="eyebrow">Rastros</span>
                    <h2>Fontes com maior peso</h2>
                  </div>
                </div>
                <ul className="metric-list">
                  {sourceHighlights.map(([source, total]) => (
                    <li key={source}>
                      <strong>{source}</strong>
                      <span>{currency(total)}</span>
                    </li>
                  ))}
                </ul>
              </div>

              <div>
                <div className="panel__header">
                  <div>
                    <span className="eyebrow">Agrupamento</span>
                    <h2>Contexto do match</h2>
                  </div>
                </div>
                <ul className="note-list">
                  {(result.match_context?.notes || []).map((note) => (
                    <li key={note}>{note}</li>
                  ))}
                </ul>
                <p className="small-muted">
                  Órgãos distintos: {(result.match_context?.distinct_orgaos || []).join(", ") || "nenhum"}.
                </p>
                <p className="small-muted">
                  Hint IDs: {(result.match_context?.distinct_person_hint_ids || []).join(", ") || "nenhum"}.
                </p>
              </div>
            </section>

            <section className="panel">
              <div className="panel__header">
                <div>
                  <span className="eyebrow">Clusters</span>
                  <h2>Resultados consolidados</h2>
                </div>
                <p>Os clusters continuam visíveis para inspeção manual quando houver ambiguidade.</p>
              </div>

              <div className="cluster-list">
                {(result.clusters || []).map((cluster) => (
                  <article className="cluster-card" key={cluster.cluster_id}>
                    <header>
                      <strong>{cluster.person_name_norm}</strong>
                      <span>{cluster.confidence}</span>
                    </header>
                    <p>
                      Município: {cluster.municipio || "n/d"} | Órgão: {cluster.orgao || "n/d"}
                    </p>
                    <small>{cluster.evidence?.rule || "sem regra"}</small>
                  </article>
                ))}
              </div>
            </section>
          </>
        ) : null}
      </main>
    </div>
  );
}
