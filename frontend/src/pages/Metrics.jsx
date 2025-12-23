import { useEffect, useMemo, useRef, useState } from "react";
import { getMetrics } from "../api/api";

const PAGE_SIZE = 100;
const PINNED_COLUMNS = ["scheme_name", "scheme_code"];

const COLUMN_GROUPS = {
  "Scheme Info": [
    "scheme_name",
    "scheme_code",
    "category",
    "fund_manager",
    "declared_benchmark",
    "fund_objective_summary",
    "asset_allocation_summary",
  ],
  Performance: ["cagr", "rolling_1y", "rolling_3y", "rolling_5y"],
  Risk: [
    "volatility_annual",
    "sharpe",
    "sortino",
    "max_drawdown",
    "beta",
    "tracking_error",
  ],
  "NAV & Size": [
    "scheme_start_date",
    "scheme_initial_nav",
    "scheme_latest_date",
    "scheme_current_nav",
    "aum",
  ],
  Other: ["data_points", "expense_ratio_percent", "exit_load"],
};

export default function Metrics({ token }) {
  const [rows, setRows] = useState([]);
  const [allColumns, setAllColumns] = useState([]);
  const [visibleColumns, setVisibleColumns] = useState([]);
  const [loading, setLoading] = useState(true);

  const [search, setSearch] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [benchmarkFilter, setBenchmarkFilter] = useState("");

  const [page, setPage] = useState(1);
  const [sortConfig, setSortConfig] = useState({ key: null, direction: null });

  const [showChooser, setShowChooser] = useState(false);
  const chooserRef = useRef(null);

  /* ---------------- LOAD ---------------- */
  useEffect(() => {
    async function load() {
      setLoading(true);
      const data = await getMetrics(token);

      const flat = [];
      Object.values(data || {}).forEach(parent =>
        parent?.children?.forEach(child => flat.push(child))
      );

      const cols = [];
      Object.values(COLUMN_GROUPS).forEach(group =>
        group.forEach(c => {
          if (!cols.includes(c)) cols.push(c);
        })
      );

      setRows(flat);
      setAllColumns(cols);
      setVisibleColumns(cols);
      setPage(1);
      setLoading(false);
    }
    load();
  }, [token]);

  /* ---------------- CLICK OUTSIDE ---------------- */
  useEffect(() => {
    function handler(e) {
      if (chooserRef.current && !chooserRef.current.contains(e.target)) {
        setShowChooser(false);
      }
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  /* ---------------- FILTER ---------------- */
  const filteredRows = useMemo(() => {
    return rows.filter(r => {
      const q = search.toLowerCase();
      return (
        (!q ||
          [r.scheme_name, r.scheme_code, r.category, r.declared_benchmark]
            .filter(Boolean)
            .some(v => String(v).toLowerCase().includes(q))) &&
        (!categoryFilter || r.category === categoryFilter) &&
        (!benchmarkFilter || r.declared_benchmark === benchmarkFilter)
      );
    });
  }, [rows, search, categoryFilter, benchmarkFilter]);

  /* ---------------- SORT ---------------- */
  const sortedRows = useMemo(() => {
    if (!sortConfig.key) return filteredRows;
    return [...filteredRows].sort((a, b) => {
      const av = a[sortConfig.key];
      const bv = b[sortConfig.key];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number") {
        return sortConfig.direction === "asc" ? av - bv : bv - av;
      }
      return sortConfig.direction === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
  }, [filteredRows, sortConfig]);

  /* ---------------- PAGINATION ---------------- */
  const totalPages = Math.ceil(sortedRows.length / PAGE_SIZE);
  const safePage = Math.min(Math.max(page, 1), totalPages || 1);
  const visibleRows = sortedRows.slice(
    (safePage - 1) * PAGE_SIZE,
    safePage * PAGE_SIZE
  );

  /* ---------------- COLUMNS ---------------- */
  const orderedColumns = useMemo(() => {
    const pinned = PINNED_COLUMNS.filter(c => visibleColumns.includes(c));
    const rest = visibleColumns.filter(c => !PINNED_COLUMNS.includes(c));
    return [...pinned, ...rest];
  }, [visibleColumns]);

  function toggleGroup(group) {
    const groupCols = COLUMN_GROUPS[group];
    const isActive = groupCols.every(c => visibleColumns.includes(c));

    setVisibleColumns(prev => {
      if (isActive) {
        return prev.filter(
          c => !groupCols.includes(c) || PINNED_COLUMNS.includes(c)
        );
      }
      return [...new Set([...prev, ...groupCols])];
    });
  }

  function handleSort(col) {
    setSortConfig(prev => {
      if (prev.key !== col) return { key: col, direction: "asc" };
      if (prev.direction === "asc") return { key: col, direction: "desc" };
      return { key: null, direction: null };
    });
  }

  /* ---------------- UI ---------------- */
  return (
    <div style={{ padding: 20 }}>
      <h2>Mutual Fund Metrics</h2>

      {/* TOP BAR */}
      <div style={{ display: "flex", gap: 10, marginBottom: 12 }}>
        <input
          placeholder="🔍 Search…"
          value={search}
          onChange={e => {
            setSearch(e.target.value);
            setPage(1);
          }}
          style={{ flex: 1, padding: 6 }}
        />

        <select
          value={benchmarkFilter}
          onChange={e => {
            setBenchmarkFilter(e.target.value);
            setPage(1);
          }}
        >
          <option value="">All Benchmarks</option>
          {[...new Set(rows.map(r => r.declared_benchmark).filter(Boolean))].map(
            b => (
              <option key={b}>{b}</option>
            )
          )}
        </select>

        {/* COLUMN CHOOSER */}
        <div style={{ position: "relative" }} ref={chooserRef}>
          <button onClick={() => setShowChooser(s => !s)}>
            Columns ▾
          </button>

          {showChooser && (
            <div
              style={{
                position: "absolute",
                right: 0,
                top: "110%",
                background: "#fff",
                border: "1px solid #ccc",
                boxShadow: "0 2px 6px rgba(0,0,0,0.15)",
                padding: 10,
                zIndex: 10,
                minWidth: 220,
              }}
            >
              {Object.keys(COLUMN_GROUPS).map(group => {
                const cols = COLUMN_GROUPS[group];
                const checked = cols.every(c =>
                  visibleColumns.includes(c)
                );
                return (
                  <div key={group} style={{ marginBottom: 6 }}>
                    <label>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleGroup(group)}
                      />{" "}
                      <strong>{group}</strong>
                    </label>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {loading && <p>⏳ Loading metrics…</p>}

      {!loading && (
        <>
          <p>
            Showing {(safePage - 1) * PAGE_SIZE + 1}–
            {Math.min(safePage * PAGE_SIZE, sortedRows.length)} of{" "}
            {sortedRows.length}
          </p>

          <div style={{ maxHeight: 600, overflow: "auto", border: "1px solid #ddd" }}>
            <table
              cellPadding="6"
              style={{ borderCollapse: "collapse", width: "max-content" }}
            >
              <thead>
                <tr>
                  {orderedColumns.map((col, i) => (
                    <th
                      key={col}
                      onClick={() => handleSort(col)}
                      style={stickyHeaderStyle(col, i)}
                    >
                      {col.toUpperCase()}
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {visibleRows.map((row, r) => (
                  <tr key={r}>
                    {orderedColumns.map((col, c) => (
                      <td key={col} style={stickyCellStyle(col, c)}>
                        {safeRender(row[col])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{ marginTop: 12 }}>
            <button disabled={safePage === 1} onClick={() => setPage(p => p - 1)}>
              ◀ Prev
            </button>
            <span style={{ margin: "0 12px" }}>
              Page {safePage} / {totalPages || 1}
            </span>
            <button
              disabled={safePage === totalPages}
              onClick={() => setPage(p => p + 1)}
            >
              Next ▶
            </button>
          </div>
        </>
      )}
    </div>
  );
}

/* ---------------- STYLES ---------------- */
function stickyHeaderStyle(col, index) {
  const pinned = PINNED_COLUMNS.includes(col);
  return {
    position: "sticky",
    top: 0,
    left: pinned ? (index === 0 ? 0 : 180) : undefined,
    background: "#f1f1f1",
    zIndex: pinned ? 4 : 3,
    minWidth: pinned ? (index === 0 ? 180 : 120) : 120,
  };
}

function stickyCellStyle(col, index) {
  if (!PINNED_COLUMNS.includes(col)) return {};
  return {
    position: "sticky",
    left: index === 0 ? 0 : 180,
    background: "#fff",
    zIndex: 2,
    minWidth: index === 0 ? 180 : 120,
  };
}

function safeRender(v) {
  if (v == null) return "-";
  if (typeof v === "number") return isFinite(v) ? v.toFixed(3) : "-";
  return String(v);
}
