# -*- coding: utf-8 -*-
import re
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from scipy.optimize import minimize_scalar

# ============== CONFIG =================
st.set_page_config(layout="wide")
st.title("📊 Relatório Consolidado de Vendas e Estoque")

CONFIG = {
    "master_keyword": "Atualizar_produto",
    "sku_col": "SKU (N)",
    "master_cols": ["SKU (N)", "Estoque (N)", "Preço de custo (N)"],
    "sales_sku_col": "SKU",
    "sales_qty_col": "Quantidade vendida",
}

MARKETPLACES = {
    "SHOPEE": {"comissao": 20.0, "imposto": 14.0, "taxa_fixa": 4.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "SHOPEE_FULL": {"comissao": 20.0, "imposto": 14.0, "taxa_fixa": 5.5, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "SHEIN": {"comissao": 16.0, "imposto": 14.0, "taxa_fixa": 6.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "SHEIN_FULL": {"comissao": 16.0, "imposto": 14.0, "taxa_fixa": 6.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "MERCADO_LIVRE_CLASSICO": {"comissao": 12.0, "imposto": 14.0, "taxa_fixa": 7.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "MERCADO_LIVRE_PREMIUM": {"comissao": 18.0, "imposto": 14.0, "taxa_fixa": 7.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "MERCADO_LIVRE_FULL_PP": {"comissao": 12.0, "imposto": 14.0, "taxa_fixa": 8.5, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "MERCADO_LIVRE_FULL_PMG": {"comissao": 12.0, "imposto": 14.0, "taxa_fixa": 10.5, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "KWAI": {"comissao": 20.0, "imposto": 14.0, "taxa_fixa": 4.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "YAMPI": {"comissao": 6.5, "imposto": 14.0, "taxa_fixa": 2.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "TIKTOK": {"comissao": 6.5, "imposto": 14.0, "taxa_fixa": 2.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "AMAZON": {"comissao": 0.0, "imposto": 14.0, "taxa_fixa": 2.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
    "PERSONALIZADO": {"comissao": 10.0, "imposto": 14.0, "taxa_fixa": 5.0, "embalagem": 0.80, "custo_fixo": 10.0, "taxa_devolucao": 0.0},
}

# ==== CONSERVATIVE ELASTICITY SETTINGS (base) ====
ELAST_SETTINGS = {
    "global_mult": 1.25,   # push E further from zero globally (more conservative)
    "up_mult": 1.25,       # if preco > preco_ref -> stronger (more negative) elasticity
    "down_mult": 1.00,     # if preco <= preco_ref -> do not over-promise lift
    "clip_min": -3.0,      # clip for SKU/global estimates
    "clip_max": -1.0,
    "eff_clip_min": -3.2,  # clip for effective elasticity in demand calc
    "eff_clip_max": -0.8,
}

def make_effective_settings(level: float):
    """Scale conservative behavior by a live knob (0.5 relax … 2.0 very conservative)."""
    base = ELAST_SETTINGS
    level = float(np.clip(level, 0.5, 2.0))
    return {
        "global_mult": base["global_mult"] * level,
        "up_mult": base["up_mult"] * level,
        "down_mult": base["down_mult"],
        "clip_min": base["clip_min"],
        "clip_max": base["clip_max"],
        "eff_clip_min": base["eff_clip_min"],
        "eff_clip_max": base["eff_clip_max"],
    }

# ============== IO & HELPERS ==============

def ler_arquivo(file):
    try:
        if file.name.lower().endswith((".xlsx", ".xls")):
            return pd.read_excel(file)
        return pd.read_csv(file)
    except Exception as e:
        st.error(f"Erro ao ler '{file.name}': {e}")
        return None

def to_float_ptbr(series: pd.Series) -> pd.Series:
    """Converte strings pt-BR '1.234,56' para float; mantém numéricos."""
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    s = (series.astype(str)
               .str.replace(r"[^\d,.\-]", "", regex=True)
               .str.replace(".", "", regex=False)    # remove milhar
               .str.replace(",", ".", regex=False))  # vírgula -> ponto
    return pd.to_numeric(s, errors="coerce")

def to_int_safe(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)

def fmt_money(x) -> str:
    try:
        return f"R$ {float(x):.2f}"
    except (ValueError, TypeError):
        return "R$ 0.00"

def fmt_qty(x) -> str:
    try:
        return f"{int(round(float(x)))}"
    except (ValueError, TypeError):
        return "0"

def extrair_token_mes_ano(nome):
    s = nome.upper()
    # YYYYMM or YYYY-MM
    m = re.search(r"(?<!\d)(\d{4})[-_]?(\d{2})(?!\d)", s)
    if m and 1 <= int(m.group(2)) <= 12:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    # MMYYYY or MM-YYYY
    m = re.search(r"(?<!\d)(\d{2})[-_]?(\d{4})(?!\d)", s)
    if m and 1 <= int(m.group(1)) <= 12:
        return f"{m.group(2)}-{int(m.group(1)):02d}"
    # Fallback: YYYY
    m = re.search(r"(?<!\d)(\d{4})(?!\d)", s)
    return m.group(1) if m else None

# ============== LEITURA E CONSOLIDAÇÃO ==============

def _combine_month_frames(prev: pd.DataFrame, new: pd.DataFrame, sku_col: str, sales_col: str, price_col: str) -> pd.DataFrame:
    """
    Combina dois DataFrames do mesmo mês:
      - Soma vendas
      - Recalcula preço médio ponderado usando SOMENTE linhas com preço conhecido
        (evita zerar/descartar preços se um lado não tem a coluna de preço).
    """
    comb = pd.concat([prev.copy(), new.copy()], ignore_index=True, sort=False)
    # Garantir colunas
    if price_col not in comb.columns:
        comb[price_col] = np.nan
    if sales_col not in comb.columns:
        comb[sales_col] = 0.0

    comb["_vq"] = comb[price_col].astype(float) * comb[sales_col].astype(float)
    comb["_w"]  = np.where(comb[price_col].notna(), comb[sales_col].astype(float), 0.0)

    out = comb.groupby(sku_col, as_index=False).agg(
        **{
            sales_col: (sales_col, "sum"),
            "_vq": ("_vq", "sum"),
            "_w": ("_w", "sum")
        }
    )
    out[price_col] = np.where(out["_w"] > 0, out["_vq"] / out["_w"], np.nan)
    return out.drop(columns=["_vq", "_w"])

def processar_arquivos(uploaded_files):
    df_master, sales_dfs = None, {}

    for file in uploaded_files:
        df = ler_arquivo(file)
        if df is None:
            continue

        # Arquivo mestre
        if CONFIG["master_keyword"] in file.name and all(c in df.columns for c in CONFIG["master_cols"]):
            df_master = df[CONFIG["master_cols"]].copy()
            df_master["Estoque (N)"] = to_int_safe(df_master["Estoque (N)"])
            df_master["Preço de custo (N)"] = to_float_ptbr(df_master["Preço de custo (N)"]).fillna(0.0)
            df_master = df_master.drop_duplicates(subset=[CONFIG["sku_col"]])
            continue

        # Arquivos de vendas
        token = extrair_token_mes_ano(file.name)
        if not token or not all(c in df.columns for c in [CONFIG["sales_sku_col"], CONFIG["sales_qty_col"]]):
            continue

        sales_col = f"Vendas_{token}"
        price_col = f"Preço_{token}"

        base = df.rename(columns={
            CONFIG["sales_sku_col"]: CONFIG["sku_col"],
            CONFIG["sales_qty_col"]: sales_col
        })[[CONFIG["sku_col"], sales_col]].copy()
        base[sales_col] = pd.to_numeric(base[sales_col], errors="coerce").fillna(0)

        # Coluna de preço (case-insensitive + sem acento)
        lower_map = {c.lower(): c for c in df.columns}
        price_candidates = [
            "preço", "preço de venda", "valor unitário", "preço unitário", "valor",
            "preco", "preco de venda", "valor unitario", "preco unitario"
        ]
        for cand in price_candidates:
            if cand in lower_map:
                base[price_col] = to_float_ptbr(df[lower_map[cand]])
                break

        # Agregar por SKU com média ponderada de preço (se existir)
        if price_col in base.columns:
            g = base[[CONFIG["sku_col"], sales_col, price_col]].copy()
            g["_v_q"] = g[price_col] * g[sales_col]
            grouped = g.groupby(CONFIG["sku_col"], as_index=False).agg({sales_col: "sum", "_v_q": "sum"})
            grouped[price_col] = np.where(grouped[sales_col] > 0, grouped["_v_q"] / grouped[sales_col], np.nan)
            grouped = grouped.drop(columns=["_v_q"])
        else:
            grouped = base.groupby(CONFIG["sku_col"], as_index=False).agg({sales_col: "sum"})

        # Se já existe este mês, combinar de forma robusta (não perder preços)
        if token in sales_dfs:
            prev = sales_dfs[token]
            sales_dfs[token] = _combine_month_frames(prev, grouped, CONFIG["sku_col"], sales_col, price_col)
        else:
            sales_dfs[token] = grouped

    return df_master, sales_dfs

def criar_relatorio_consolidado(df_master, sales_dfs):
    df = df_master.copy()
    meses = sorted(sales_dfs.keys())
    for m in meses:
        df = df.merge(sales_dfs[m], on=CONFIG["sku_col"], how="left")
    sales_cols = [f"Vendas_{m}" for m in meses]
    price_cols = [f"Preço_{m}" for m in meses if f"Preço_{m}" in df.columns]
    if sales_cols:
        df[sales_cols] = df[sales_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
    if price_cols:
        df[price_cols] = df[price_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return df, meses

# ============== VISUALIZAÇÕES ==============

def exibir_resumo_performance(df, meses):
    with st.expander("📈 Resumo de Performance Mensal", expanded=False):
        if len(meses) < 2:
            st.info("Envie pelo menos 2 arquivos de vendas.")
            return
        cur, prev = f"Vendas_{meses[-1]}", f"Vendas_{meses[-2]}"
        if cur not in df.columns: df[cur] = 0
        if prev not in df.columns: df[prev] = 0
        aux = df[[CONFIG["sku_col"], prev, cur]].copy()
        aux["Variação"] = aux[cur] - aux[prev]
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🔝 Top 5 Aumentos")
            st.dataframe(aux.nlargest(5, "Variação"), hide_index=True, use_container_width=True)
        with c2:
            st.subheader("🔻 Top 5 Quedas")
            st.dataframe(aux.nsmallest(5, "Variação"), hide_index=True, use_container_width=True)

def exibir_grafico_tendencias(df, meses):
    with st.expander("📉 Tendências de Vendas", expanded=False):
        if not meses:
            st.info("Sem meses consolidados.")
            return
        skus = df[CONFIG["sku_col"]].dropna().unique()
        sel = st.multiselect("Selecione os SKUs:", skus, default=list(skus[:3]) if len(skus) >= 3 else list(skus))
        if not sel:
            return
        cols = [f"Vendas_{m}" for m in meses]
        melted = (df[df[CONFIG["sku_col"]].isin(sel)]
                  .melt(id_vars=CONFIG["sku_col"], value_vars=cols, var_name="Mês", value_name="Vendas")
                  .replace({"Mês": {f"Vendas_{m}": m for m in meses}}))
        chart_df = melted.pivot(index="Mês", columns=CONFIG["sku_col"], values="Vendas").fillna(0)
        chart_df = chart_df.sort_index()
        st.line_chart(chart_df)

# ============== PRECIFICAÇÃO: CORE ==============

def calcular_preco_para_margem(custo, margem_pct, fx_pct, com_pct, emb, taxa, imp_pct, dev_pct=0.0):
    custos_fixos_reais = float(custo) + float(emb) + float(taxa)
    tot = (float(fx_pct)+float(com_pct)+float(imp_pct)+float(dev_pct)+float(margem_pct))/100.0
    if tot >= 1:
        raise ValueError(f"Percentuais somam {tot*100:.1f}% (deve ser < 100%)")
    return custos_fixos_reais/(1.0 - tot)

def calcular_custos(custo, preco, cfg_fixo, cfg_com, cfg_emb, cfg_taxa, cfg_imp, cfg_dev=0.0):
    custo, preco = float(custo), float(preco)
    if preco <= 0:
        return {"custo_produto": custo, "custo_fixo":0.0, "comissao":0.0, "embalagem":0.0, "taxa_fixa":0.0,
                "imposto":0.0, "taxa_devolucao":0.0, "custo_total":custo, "lucro":0.0, "margem_liquida":0.0}
    v_fixo = preco*(float(cfg_fixo)/100.0)
    v_com  = preco*(float(cfg_com)/100.0)
    v_imp  = preco*(float(cfg_imp)/100.0)
    v_dev  = preco*(float(cfg_dev)/100.0)
    total  = custo + v_fixo + v_com + float(cfg_emb) + float(cfg_taxa) + v_imp + v_dev
    lucro  = preco - total
    margem = (lucro/preco)*100.0 if preco > 0 else 0.0
    return {"custo_produto":custo,"custo_fixo":v_fixo,"comissao":v_com,"embalagem":float(cfg_emb),
            "taxa_fixa":float(cfg_taxa),"imposto":v_imp,"taxa_devolucao":v_dev,
            "custo_total":total,"lucro":lucro,"margem_liquida":margem}

# ---- Elasticidade (global + shrink por SKU)

def _arc_change(x1, x2):
    x1, x2 = float(x1), float(x2); den = (x1 + x2) / 2.0
    return 0.0 if den == 0 else (x2 - x1) / den

def calcular_elasticidade_global(R1, Q1, R2, Q2, pct_preco_declarado=None):
    Q1, Q2 = float(Q1), float(Q2)
    if Q1 <= 0 or Q2 <= 0: return -1.2, {"ok": False, "msg": "Q1/Q2 > 0"}
    dQ = _arc_change(Q1, Q2)
    if pct_preco_declarado is None:
        P1, P2 = float(R1)/Q1, float(R2)/Q2
        dP = _arc_change(P1, P2); src = "dados (P=R/Q)"
    else:
        g = float(pct_preco_declarado)/100.0
        dP = g/(1.0 + g/2.0); P1 = float(R1)/Q1; P2 = (1.0 + g)*P1; src = "variação declarada"
    if dP == 0: return -1.2, {"ok": False, "msg": "ΔP=0", "src": src}
    E = dQ/dP
    return float(E), {"ok": True, "src": src, "dQ": dQ, "dP": dP, "P1": float(P1), "P2": float(P2)}

def elasticidade_por_cv(cv, clip_min, clip_max):
    cv = max(0.0, float(cv)); k = min(1.0, cv/0.6)
    e = -1.2 - 1.8*k
    return float(np.clip(e, clip_min, clip_max))

def estimar_elasticidade_melhorada(vendas_mensais, E_global, w_global=0.60, clip_min=-3.0, clip_max=-1.0):
    v = [float(x) for x in vendas_mensais if float(x) > 0]
    if len(v) < 2 or np.mean(v) <= 0:
        return float(np.clip(E_global, clip_min, clip_max))
    cv = float(np.std(v)/np.mean(v))
    e_prior_sku = elasticidade_por_cv(cv, clip_min, clip_max)
    e_sku = w_global*float(E_global) + (1.0 - w_global)*e_prior_sku
    return float(np.clip(e_sku, clip_min, clip_max))

# ---- Demanda + Otimização com guardrails

def demanda_sem_preco_historico(preco, preco_ref, qtd_ref, E, settings=None):
    """Price-response with asymmetric, conservative elasticity."""
    if settings is None:
        settings = ELAST_SETTINGS
    preco, preco_ref = float(preco), float(preco_ref)
    if preco <= 0 or preco_ref <= 0:
        return 0.0
    mult = settings["up_mult"] if preco > preco_ref else settings["down_mult"]
    E_eff = float(E) * float(mult)
    E_eff = float(np.clip(E_eff, settings["eff_clip_min"], settings["eff_clip_max"]))
    return float(qtd_ref) * ((preco/preco_ref) ** E_eff)

def calcular_preco_otimo_sem_historico(custo, preco_ref, qtd_media, E, margem_min, cfg, retencao_min_pct, limite_var_pct, settings=None):
    if settings is None:
        settings = ELAST_SETTINGS
    # limites
    try:
        p_be = calcular_preco_para_margem(custo, 0.0, cfg["cfg_fixo"], cfg["cfg_com"], cfg["cfg_emb"], cfg["cfg_taxa"], cfg["cfg_imp"], cfg.get("cfg_dev", 0.0))
    except Exception:
        p_be = float(custo) * 1.5
    lb = max(p_be*1.05, float(custo)*1.2, float(preco_ref) * (1.0 - float(limite_var_pct)/100.0))
    ub = min(float(preco_ref) * (1.0 + float(limite_var_pct)/100.0), max(float(preco_ref)*2.0, lb*1.2))
    if ub <= lb: ub = lb * 1.1
    retencao_min = float(retencao_min_pct)/100.0
    demanda_ref = float(qtd_media)

    def objetivo(p):
        c = calcular_custos(custo, p, **cfg)
        if c["margem_liquida"] < float(margem_min): return float("inf")
        d = demanda_sem_preco_historico(p, preco_ref, qtd_media, E, settings=settings)
        if d < retencao_min * demanda_ref: return float("inf")
        return -c["lucro"] * d

    res = minimize_scalar(objetivo, bounds=(lb, ub), method="bounded")
    if res.success and np.isfinite(objetivo(res.x)):
        return float(res.x), lb, ub, p_be

    # fallback: busca em grade
    grid = np.linspace(lb, ub, 120)
    vals = [(objetivo(p), p) for p in grid]
    vals = [t for t in vals if np.isfinite(t[0])]
    if vals:
        best = min(vals, key=lambda t: t[0])
        return float(best[1]), lb, ub, p_be
    return float(preco_ref), lb, ub, p_be

# ============== UI AUXILIARES ==============

def _add_hline_on_y3(fig, x0, x1, y, text, color="gray", dash="dot", opacity=0.5):
    fig.add_shape(
        type="line", xref="x", yref="y3",
        x0=x0, x1=x1, y0=y, y1=y,
        line=dict(color=color, dash=dash, width=1),
        opacity=opacity
    )
    fig.add_annotation(
        x=x1, y=y, xref="x", yref="y3",
        text=text, showarrow=False,
        xanchor="right", yanchor="bottom",
        font=dict(size=11, color=color)
    )

def calibrar_elasticidade_global_ui(settings):
    with st.expander("🧭 Calibração de Elasticidade Global", expanded=False):
        c1, c2, c3 = st.columns([1.3, 1.3, 1.1])
        with c1:
            R1 = st.number_input("Faturamento Período A (R$)", min_value=0.0, value=1092950.35, step=1000.0, format="%.2f")
            Q1 = st.number_input("Vendas Período A (un)", min_value=1.0, value=19266.0, step=100.0, format="%.0f")
            D1 = st.number_input("Dias do Período A", min_value=1, value=23, step=1)
        with c2:
            R2 = st.number_input("Faturamento Período B (R$)", min_value=0.0, value=1129950.91, step=1000.0, format="%.2f")
            Q2 = st.number_input("Vendas Período B (un)", min_value=1.0, value=19143.0, step=100.0, format="%.0f")
            D2 = st.number_input("Dias do Período B", min_value=1, value=23, step=1)
        with c3:
            g_decl = st.number_input("Variação declarada de preço (%)", min_value=0.0, max_value=50.0, value=3.0, step=0.1)
            # Conservative defaults (prior & weight)
            E_prior = st.number_input("Prior varejo (elast.)", min_value=-5.0, max_value=-0.5, value=-1.8, step=0.1)
            w_prior = st.slider("Peso no prior", 0.0, 1.0, 0.80, 0.05)

        # Normaliza por dia
        R1d, Q1d = (R1 / D1), (Q1 / D1)
        R2d, Q2d = (R2 / D2), (Q2 / D2)

        E_dados, _ = calcular_elasticidade_global(R1d, Q1d, R2d, Q2d, pct_preco_declarado=None)
        E_decl, _  = calcular_elasticidade_global(R1d, Q1d, R2d, Q2d, pct_preco_declarado=g_decl)
        E_data_avg = (E_dados + E_decl) / 2.0
        E_global   = w_prior * E_prior + (1.0 - w_prior) * E_data_avg
        # Apply conservative global multiplier from current settings
        E_global   = float(np.clip(E_global * settings["global_mult"], settings["clip_min"], settings["clip_max"]))

        st.info(
            f"**Calibração**  \n"
            f"- Elasticidade (dados, P=R/Q): **{E_dados:.2f}**  \n"
            f"- Elasticidade (variação declarada de preço): **{E_decl:.2f}**  \n"
            f"- Média: **{E_data_avg:.2f}**  \n"
            f"- Prior / Peso: **{E_prior:.2f} / {w_prior:.2f}**  \n"
            f"- Multiplicador global de conservadorismo: **×{settings['global_mult']:.2f}**  \n"
            f"- **E_global (conservador) = {E_global:.2f}**  \n"
            f"_Obs.: métricas normalizadas por dia (A: {D1}d, B: {D2}d)._"
        )
    return E_global

# ============== ANÁLISE DE PRECIFICAÇÃO (UI) ==============

def exibir_analise_precificacao(df, meses):
    with st.expander("💰 Análise de Precificação por Marketplace", expanded=False):

        # 0) Knob de conservadorismo
        with st.expander("🎛️ Nível de Conservadorismo", expanded=False):
            cons_level = st.slider("Conservadorismo (↓ relaxado • ↑ rígido)", 0.5, 2.0, 1.0, 0.05,
                                   help="Escala multiplicadores conservadores: volume cai mais quando o preço sobe.")
            settings = make_effective_settings(cons_level)
            st.caption(
                f"- Multiplicador global E: ×{settings['global_mult']:.2f}  \n"
                f"- Penalização em aumento de preço (E↑ mais negativo): ×{settings['up_mult']:.2f}  \n"
                f"- Penalização em queda de preço: ×{settings['down_mult']:.2f}"
            )

        # 1) Elasticidade Global (big picture) — usa settings atuais
        E_global = calibrar_elasticidade_global_ui(settings)

        # 2) Marketplace e custos
        c1, c2 = st.columns([1, 2])
        with c1:
            marketplace = st.selectbox("Marketplace:", list(MARKETPLACES.keys()))
        with c2:
            info = MARKETPLACES[marketplace]
            st.info(f"**{marketplace}** — % totais: **{info['custo_fixo']+info['comissao']+info['imposto']:.1f}%**  •  "
                    f"Taxas fixas: **{fmt_money(info['embalagem']+info['taxa_fixa'])}**")

        st.subheader("⚙️ Custos do Marketplace")
        with st.expander("Ajustar (opcional)", expanded=False):
            a, b, c, d, e, f = st.columns(6)
            cfg = {
                "cfg_fixo": a.number_input("CUSTO_FIXO (%)", 0.0, 100.0, info["custo_fixo"], 0.5),
                "cfg_com":  b.number_input("COMISSÃO (%)",   0.0,  50.0, info["comissao"],   0.5),
                "cfg_emb":  c.number_input("EMBALAGEM (R$)", 0.0, 100.0, info["embalagem"],  0.10),
                "cfg_taxa": d.number_input("TAXA_FIXA (R$)", 0.0,  50.0, info["taxa_fixa"],  0.50),
                "cfg_dev":  e.number_input("DEVOLUÇÃO (%)",  0.0,  20.0, info.get("taxa_devolucao", 0.0), 0.5),
                "cfg_imp":  f.number_input("IMPOSTO (%)",    0.0,  50.0, info["imposto"],    0.5),
            }

        # 3) SKU e dados
        options = list(df[CONFIG["sku_col"]].dropna().unique())
        sku = st.selectbox("SKU:", options) if options else None
        if sku is None:
            st.warning("Não há SKUs disponíveis para análise.")
            return

        row = df[df[CONFIG["sku_col"]] == sku].iloc[0]
        custo_base = float(row["Preço de custo (N)"])
        estoque = int(float(row["Estoque (N)"])) if not pd.isna(row["Estoque (N)"]) else 0
        sales_cols = [f"Vendas_{m}" for m in meses]
        vendas = [float(v) for v in (row[sales_cols].values if sales_cols else [])]
        vendas_validas = [v for v in vendas if v > 0]
        qtd_media = float(np.mean(vendas_validas)) if vendas_validas else 1.0
        qtd_total = float(sum(vendas)) if vendas else 0.0

        # 3.1) Custo manual (opcional)
        st.subheader("🧾 Dados do SKU")
        kc1, kc2 = st.columns(2)
        with kc1:
            usar_custo_manual = st.checkbox("Definir custo manualmente", value=False)
        with kc2:
            custo_manual = st.number_input("Custo manual (R$)", min_value=0.0, value=float(custo_base), step=0.01, disabled=not usar_custo_manual)
        custo = custo_manual if usar_custo_manual else custo_base

        # 4) Preço alvo e margem desejada
        st.subheader("🎯 Preço Alvo e Margem Desejada")
        d1, d2 = st.columns(2)
        with d1:
            margem_desejada = st.slider("Margem desejada (%)", 1.0, 80.0, 15.0, 0.5)
        with d2:
            try:
                preco_calc = calcular_preco_para_margem(custo, margem_desejada, cfg["cfg_fixo"], cfg["cfg_com"],
                                                        cfg["cfg_emb"], cfg["cfg_taxa"], cfg["cfg_imp"], cfg["cfg_dev"])
            except ValueError as e:
                st.error(f"Erro no cálculo: {e}")
                preco_calc = custo * 3.0
            preco_alvo = st.number_input("Preço de Venda Alvo (R$)", min_value=float(custo*1.1), max_value=float(custo*10.0),
                                         value=float(preco_calc), step=0.01)
        st.caption(f"Preço = (Custo + Embalagem + Taxa) / (1 - %fixo - %comissão - %imposto - %devolução - %margem) = {fmt_money(preco_calc)}")

        # 5) Guardrails e preferências
        st.subheader("🛡️ Guardrails de Otimização")
        g1, g2, g3 = st.columns(3)
        margem_min   = g1.slider("Margem mínima (%)", 1.0, 50.0, min(10.0, margem_desejada), 0.5)
        retencao_min = g2.slider("Retenção mínima de volume vs preço-alvo (%)", 50, 100, 85, 1)
        limite_var   = g3.slider("Limite máx. de variação do preço (%)", 5, 50, 25, 1)

        # 6) Elasticidade por SKU (global + shrink) com opção manual
        st.subheader("📐 Elasticidade")
        e1, e2 = st.columns([1.2, 1])
        with e1:
            w_global = st.slider("Peso do E_global no SKU", 0.0, 1.0, 0.60, 0.05)
            elast_auto = estimar_elasticidade_melhorada(
                vendas, E_global, w_global=w_global,
                clip_min=ELAST_SETTINGS["clip_min"], clip_max=ELAST_SETTINGS["clip_max"]
            )
            st.info(f"Elasticidade estimada (SKU): **{elast_auto:.2f}**  •  E_global: {E_global:.2f}")
        with e2:
            usar_manual = st.checkbox("Ajustar elasticidade manualmente", value=False)
            elasticidade = st.slider("Elasticidade manual", -5.0, -0.5, float(elast_auto), 0.1) if usar_manual else float(elast_auto)

        # 7) Otimização com guardrails
        preco_otimo, lb, ub, p_be = calcular_preco_otimo_sem_historico(
            custo, preco_alvo, qtd_media, elasticidade, margem_min, cfg,
            retencao_min_pct=retencao_min, limite_var_pct=limite_var, settings=settings
        )
        custos_alvo = calcular_custos(custo, preco_alvo, **cfg)
        custos_otm  = calcular_custos(custo, preco_otimo, **cfg)
        demanda_alvo = demanda_sem_preco_historico(preco_alvo, preco_alvo, qtd_media, elasticidade, settings=settings)  # = qtd_media
        demanda_otm  = demanda_sem_preco_historico(preco_otimo, preco_alvo, qtd_media, elasticidade, settings=settings)

        # 8) Métricas (formatadas)
        st.subheader("📊 Resultados")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Custo Produto", fmt_money(custo))
        m1.metric("Estoque", fmt_qty(estoque) + " un")
        m2.metric("Preço Alvo", fmt_money(preco_alvo))
        m2.metric("Margem Alvo", f"{custos_alvo['margem_liquida']:.2f}%")
        m3.metric("Preço Ótimo", fmt_money(preco_otimo), f"{preco_otimo - preco_alvo:+.2f}")
        m3.metric("Margem Ótima", f"{custos_otm['margem_liquida']:.2f}%")
        m4.metric("Vendas Médias/Mês", fmt_qty(qtd_media) + " un")
        m4.metric("Vendas Totais", fmt_qty(qtd_total) + " un")

        # Avisos
        if abs(custos_alvo["margem_liquida"] - margem_desejada) > 0.5:
            st.warning(f"⚠️ Margem no preço alvo = {custos_alvo['margem_liquida']:.2f}% (≠ {margem_desejada:.2f}%)")
        if custos_alvo["margem_liquida"] < margem_min:
            st.error(f"⚠️ Margem atual ({custos_alvo['margem_liquida']:.2f}%) < mínima ({margem_min:.2f}%)")
        bound_hit = (abs(preco_otimo - lb) < 1e-6) or (abs(preco_otimo - ub) < 1e-6)
        if bound_hit:
            st.info("ℹ️ Preço ótimo no limite permitido (provável ativação de guardrails).")

        # 9) Curva lucro/demanda/margem (tema preto + limpo)
        with st.expander("📉 Curva de Otimização", expanded=False):
            # Opções do gráfico
            o1, o2, o3 = st.columns(3)
            with o1:
                show_demand = st.checkbox("Mostrar Demanda", value=False)
            with o2:
                show_margin = st.checkbox("Mostrar Margem (%)", value=True)
            with o3:
                show_guides = st.checkbox("Linhas-guia", value=True)

            # Faixa de preços
            pmin = max(p_be*0.8, custo*1.1, preco_alvo*(1 - limite_var/100))
            pmax = min(preco_alvo*(1 + limite_var/100), max(preco_alvo*2.0, pmin*1.2))
            if pmax <= pmin:
                pmax = pmin * 1.1

            xs = np.linspace(pmin, pmax, 150)
            ds, ps, ms = [], [], []
            for p in xs:
                d = demanda_sem_preco_historico(p, preco_alvo, qtd_media, elasticidade, settings=settings)
                c = calcular_custos(custo, p, **cfg)
                ds.append(d); ps.append(c["lucro"]*d); ms.append(c["margem_liquida"])

            fig = go.Figure()

            # cores legíveis no fundo preto
            color_profit = "#00F5A0"   # ciano
            color_margin = "#FFB300"   # amarelo
            color_demand = "#40C4FF"   # cinza claro

            if show_demand:
                fig.add_trace(go.Scatter(
                    x=xs, y=ds, name="Demanda Estimada", yaxis="y",
                    mode="lines", line=dict(width=1, color=color_demand), opacity=0.7
                ))

            fig.add_trace(go.Scatter(
                x=xs, y=ps, name="Lucro Total", yaxis="y2",
                mode="lines", line=dict(width=3, color=color_profit)
            ))

            if show_margin:
                fig.add_trace(go.Scatter(
                    x=xs, y=ms, name="Margem Líquida (%)", yaxis="y3",
                    mode="lines", line=dict(width=2, dash="dash", color=color_margin)
                ))

            # Marcadores de alvo/ótimo
            if show_demand:
                y_pt_otimo, y_pt_alvo, yaxis_marker = demanda_otm, demanda_alvo, "y"
                marker_color = color_demand
            elif show_margin:
                y_pt_otimo, y_pt_alvo, yaxis_marker = custos_otm["margem_liquida"], custos_alvo["margem_liquida"], "y3"
                marker_color = color_margin
            else:
                y_pt_otimo = custos_otm["lucro"] * demanda_otm
                y_pt_alvo  = custos_alvo["lucro"] * demanda_alvo
                yaxis_marker = "y2"
                marker_color = color_profit

            fig.add_trace(go.Scatter(
                x=[preco_otimo], y=[y_pt_otimo], name="Preço Ótimo",
                mode="markers", marker=dict(size=12, symbol="star", color=marker_color), yaxis=yaxis_marker
            ))
            fig.add_trace(go.Scatter(
                x=[preco_alvo], y=[y_pt_alvo], name="Preço Alvo",
                mode="markers", marker=dict(size=10, symbol="diamond", color=marker_color), yaxis=yaxis_marker
            ))

            # Linhas-guia
            if show_guides and show_margin:
                _add_hline_on_y3(fig, pmin, pmax, 0.0, "Break-even margem", color="#FF6666", opacity=0.45)
                _add_hline_on_y3(fig, pmin, pmax, margem_min, f"Margem mín {margem_min:.0f}%", color="#FFB84D", opacity=0.45)
                _add_hline_on_y3(fig, pmin, pmax, margem_desejada, f"Margem alvo {margem_desejada:.0f}%", color="#9CEC5B", opacity=0.45)

            # Layout dark/clean
            fig.update_layout(
                xaxis=dict(
                    title=dict(text="Preço (R$)", font=dict(color="#FFFFFF")),
                    range=[pmin, pmax],
                    showgrid=False, zeroline=False,
                    showline=True, linecolor="#666666", mirror=True,
                    tickfont=dict(color="#FFFFFF")
                ),
                yaxis=dict(  # Demanda
                    title=dict(text="Demanda (un)", font=dict(color="#FFFFFF")),
                    showgrid=False if not show_demand else True,
                    gridcolor="#333333", zeroline=False,
                    tickformat=",.0f",
                    tickfont=dict(color="#FFFFFF")
                ),
                yaxis2=dict(  # Lucro
                    title=dict(text="Lucro Total (R$)", font=dict(color="#FFFFFF")),
                    side="right", overlaying="y",
                    showgrid=False, zeroline=False, tickprefix="R$ ", tickformat=",.2f",
                    tickfont=dict(color="#FFFFFF")
                ),
                yaxis3=dict(  # Margem
                    title=dict(text="Margem (%)", font=dict(color="#FFFFFF")),
                    overlaying="y", side="right", position=0.95, anchor="free",
                    showgrid=False, zeroline=False, ticksuffix="%", tickformat=".1f",
                    tickfont=dict(color="#FFFFFF")
                ),
                paper_bgcolor="#000000", plot_bgcolor="#000000",
                font=dict(color="#FFFFFF"),
                height=540, hovermode="x unified", showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#FFFFFF")),
                margin=dict(l=60, r=60, t=40, b=40)
            )
            st.plotly_chart(fig, use_container_width=True)

        # 10) Cenários (formatados)
        with st.expander("📋 Cenários", expanded=False):
            try:
                p_be_calc = calcular_preco_para_margem(custo, 0.0, cfg["cfg_fixo"], cfg["cfg_com"], cfg["cfg_emb"], cfg["cfg_taxa"], cfg["cfg_imp"], cfg["cfg_dev"])
            except Exception:
                p_be_calc = custo*1.5
            cenarios = {
                "Preço Alvo": preco_alvo,
                "Preço Ótimo": preco_otimo,
                "Break-even": p_be_calc,
                "Alvo -10%": preco_alvo*0.9,
                "Alvo +10%": preco_alvo*1.1,
                "Alvo +20%": preco_alvo*1.2
            }
            rows = []
            for nome, p in cenarios.items():
                d = demanda_sem_preco_historico(p, preco_alvo, qtd_media, elasticidade, settings=settings)
                c = calcular_custos(custo, p, **cfg)
                rows.append({
                    "Cenário": nome,
                    "Preço": fmt_money(p),
                    "Demanda Est.": fmt_qty(d),
                    "Lucro Unit.": fmt_money(c["lucro"]),
                    "Lucro Total": fmt_money(c["lucro"]*d),
                    "Receita": fmt_money(p*d),
                    "Margem": f"{c['margem_liquida']:.2f}%"
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

        # 11) Recomendação final (formatada)
        with st.expander("💡 Recomendação", expanded=False):
            lucro_otm = custos_otm["lucro"] * demanda_otm
            lucro_alv = custos_alvo["lucro"] * demanda_alvo
            c1, c2 = st.columns(2)
            with c1:
                st.success(
                    f"**Preço recomendado: {fmt_money(preco_otimo)}**\n\n"
                    f"- Margem líquida: **{custos_otm['margem_liquida']:.2f}%**\n"
                    f"- Lucro unit.: **{fmt_money(custos_otm['lucro'])}**\n"
                    f"- Demanda est.: **{fmt_qty(demanda_otm)} un/mês**\n"
                    f"- Lucro total: **{fmt_money(lucro_otm)}/mês**\n"
                    f"- Receita: **{fmt_money(preco_otimo*demanda_otm)}/mês**"
                )
            with c2:
                diff = preco_otimo - preco_alvo
                pct  = 100.0*diff/preco_alvo if preco_alvo > 0 else 0.0
                ganho = lucro_otm - lucro_alv
                if abs(pct) < 5:
                    st.success(f"✅ Preço alvo bem posicionado (ajuste sugerido {abs(pct):.1f}%).")
                elif diff > 0:
                    st.warning(
                        f"📈 Aumentar preço em **{pct:.1f}%** ({fmt_money(diff)})  \n"
                        f"Ganho de lucro estimado: **{fmt_money(ganho)}/mês**  \n"
                        f"Demanda: {fmt_qty(demanda_alvo)} → {fmt_qty(demanda_otm)} un/mês"
                    )
                else:
                    st.info(
                        f"📉 Reduzir preço em **{abs(pct):.1f}%** ({fmt_money(abs(diff))})  \n"
                        f"Ganho de lucro estimado: **{fmt_money(ganho)}/mês**  \n"
                        f"Demanda: {fmt_qty(demanda_alvo)} → {fmt_qty(demanda_otm)} un/mês"
                    )
                if custos_alvo["margem_liquida"] < margem_min:
                    st.error(f"⚠️ Margem atual ({custos_alvo['margem_liquida']:.2f}%) < mínima ({margem_min:.2f}%).")

# ============== APP ==============

uploaded_files = st.file_uploader("📂 Arraste seus arquivos aqui", type=["csv", "xlsx", "xls"], accept_multiple_files=True)
if not uploaded_files:
    st.info("⏳ Aguardando arquivos...")
else:
    df_master, sales_dfs = processar_arquivos(uploaded_files)
    if df_master is None:
        st.warning("⚠️ Arquivo principal não encontrado (deve conter 'Atualizar_produto' e colunas esperadas).")
    elif not sales_dfs:
        st.warning("⚠️ Nenhum arquivo de vendas válido.")
    else:
        df_final, meses = criar_relatorio_consolidado(df_master, sales_dfs)

        # 📈 Resumo
        exibir_resumo_performance(df_final, meses)

        # 📄 Relatório Consolidado (formatado para exibição)
        with st.expander("📄 Relatório Consolidado", expanded=False):
            df_disp = df_final.copy()
            # Quantidades
            if "Estoque (N)" in df_disp.columns:
                df_disp["Estoque (N)"] = to_int_safe(df_disp["Estoque (N)"])
            for m in meses:
                col_v = f"Vendas_{m}"
                if col_v in df_disp.columns:
                    df_disp[col_v] = to_int_safe(df_disp[col_v])
            # Monetários
            if "Preço de custo (N)" in df_disp.columns:
                df_disp["Preço de custo (N)"] = df_disp["Preço de custo (N)"].apply(fmt_money)
            for m in meses:
                col_p = f"Preço_{m}"
                if col_p in df_disp.columns:
                    df_disp[col_p] = df_final[col_p].apply(lambda v: fmt_money(v) if pd.notna(v) else "—")
            st.dataframe(df_disp, use_container_width=True)

        # 📉 Tendências
        exibir_grafico_tendencias(df_final, meses)

        # 💰 Precificação
        if not df_final.empty:
            exibir_analise_precificacao(df_final, meses)
        else:
            st.warning("Não há dados consolidados para a análise de precificação.")
