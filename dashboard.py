import streamlit as st
import requests
import pandas as pd
import os
import altair as alt
import multiprocessing
from streamlit_autorefresh import st_autorefresh
import subprocess

# Configuração da página
st.set_page_config(
    page_title="Pipeline Controller",
    layout="wide",                     # ocupa toda a tela
    initial_sidebar_state="expanded",  # sidebar sempre aberto
    menu_items={
        'Get help': None,              # opcional: limpa o menu de ajuda
        'Report a bug': None,
        'About': None
    }
)
st.title("💼 Dashboard: Execução do Pipeline")

# === 1) MAPEAMENTOS DE CORES FIXAS ===
event_colors = {
    "play":    "#66c2a5",
    "pause":   "#fc8d62",
    "stop":    "#8da0cb",
    "search":  "#e78ac3",
    "login":   "#a6d854",
    "logout":  "#ffd92f",
    "like":    "#e5c494",
    "dislike": "#b3b3b3",
    "skip_ad": "#1b9e77",
}

genre_colors = {
    "action":    "#e41a1c",
    "adventure": "#ff7f0e",
    "comedy":    "#fdae61",
    "drama":     "#377eb8",
    "horror":    "#4d4d4d",
    "mystery":   "#984ea3",
    "romance":   "#f781bf",
    "sci-fi":    "#17becf",
    "sports":    "#4daf4a",
    "thriller":  "#800000",
}

# === 2) CONTROLES DE PROCESSOS E AUTO-REFRESH ===
max_cores = multiprocessing.cpu_count()
num_processes = st.sidebar.slider(
    "Número de processos:",
    min_value=1,
    max_value=max_cores,
    value=min(4, max_cores),
    help="Define quantos processos paralelos o pipeline deve usar."
)

st_autorefresh(interval=5000, key="auto-refresh")
status_placeholder = st.empty()
log_placeholder = st.empty()

def auto_trigger_pipeline(processes: int):
    try:
        res = requests.post("http://localhost:5000/trigger_pipeline", json={"num_processes": processes})
        if res.status_code == 202:
            st.success(f"✅ Pipeline iniciado com {processes} processos.")
        elif res.status_code == 409:
            st.info("⚠️ Pipeline já em execução.")
        else:
            st.error(f"❌ Erro ao iniciar: {res.status_code}")
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Erro de conexão: {e}")

if st.sidebar.button("🔄 Reiniciar Pipeline"):
    auto_trigger_pipeline(num_processes)
else:
    auto_trigger_pipeline(num_processes)

def fetch_status():
    try:
        r = requests.get("http://localhost:5000/status")
        if r.status_code == 200:
            return r.json()
    except:
        return None

status = fetch_status()
if status:
    text = "🔄 Rodando..." if status.get("is_running") else "🟢 Pronto para execução"
    status_placeholder.markdown(f"### Status atual: {text}")
    logs = "\n".join(status.get("last_logs", []))
    log_placeholder.text_area("📋 Logs recentes:", logs, height=300)
else:
    status_placeholder.markdown("### ❌ Não foi possível conectar ao servidor.")

# === 3) HISTOGRAMA: Contagem de eventos ===
st.subheader("📊 Distribuição dos eventos na última hora")
evt_path = os.path.join("transformed_data", "event_count_last_hour.csv")
if os.path.exists(evt_path):
    df_e = pd.read_csv(evt_path)
    if not df_e.empty and {"event", "quantidade"}.issubset(df_e.columns):
        df_e = df_e.sort_values("quantidade", ascending=False)
        chart_events = (
            alt.Chart(df_e).mark_bar()
            .encode(
                x=alt.X(
                    "event:N", title="Evento",
                    sort=alt.EncodingSortField("quantidade", order="descending")
                ),
                y=alt.Y("quantidade:Q", title="Quantidade"),
                color=alt.Color(
                    "event:N", title="Evento",
                    scale=alt.Scale(
                        domain=list(event_colors.keys()),
                        range=list(event_colors.values())
                    )
                ),
                tooltip=["event", "quantidade"]
            )
            .properties(width=700, height=400)
        )
        st.altair_chart(chart_events, use_container_width=True)
    else:
        st.info("CSV de eventos vazio ou mal formatado.")
else:
    st.info("Arquivo event_count_last_hour.csv não encontrado.")




# === 4) HISTOGRAMAS DE GÊNERO LADO A LADO ===
col1, col2 = st.columns(2)

# — 4a: visualizações nas últimas 24h
with col1:
    st.subheader("📊 Visualizações por gênero (últimas 24h)")
    path_g24 = os.path.join("transformed_data", "genre_views_last_24h.csv")
    if os.path.exists(path_g24):
        df_g24 = pd.read_csv(path_g24)
        if not df_g24.empty and {"genre", "views"}.issubset(df_g24.columns):
            df_g24 = df_g24.sort_values("views", ascending=False)
            chart_g24 = (
                alt.Chart(df_g24).mark_bar()
                .encode(
                    x=alt.X(
                        "genre:N",
                        title="Gênero",
                        sort=alt.EncodingSortField("views", order="descending")
                    ),
                    y=alt.Y("views:Q", title="Visualizações"),
                    color=alt.Color(
                        "genre:N",
                        title="Gênero",
                        scale=alt.Scale(
                            domain=list(genre_colors.keys()),
                            range=list(genre_colors.values())
                        )
                    ),
                    tooltip=["genre", "views"]
                )
                .properties(
                    height=400  # mesma altura para ambos
                )
            )
            st.altair_chart(chart_g24, use_container_width=True)
        else:
            st.info("CSV genre_views_last_24h.csv vazio ou mal formatado.")
    else:
        st.info("Arquivo genre_views_last_24h.csv não encontrado.")

# — 4b: filmes não-finalizados
with col2:
    st.subheader("📊 Filmes não-finalizados por gênero")
    path_unf = os.path.join("transformed_data", "unfinished_by_genre.csv")
    if os.path.exists(path_unf):
        df_unf = pd.read_csv(path_unf)
        if not df_unf.empty and {"content_genre", "unfinished_views"}.issubset(df_unf.columns):
            df_unf = df_unf.sort_values("unfinished_views", ascending=False)
            chart_unf = (
                alt.Chart(df_unf).mark_bar()
                .encode(
                    x=alt.X(
                        "content_genre:N",
                        title="Gênero",
                        sort=alt.EncodingSortField("unfinished_views", order="descending")
                    ),
                    y=alt.Y("unfinished_views:Q", title="Visualizações"),
                    color=alt.Color(
                        "content_genre:N",
                        title="Gênero",
                        scale=alt.Scale(
                            domain=list(genre_colors.keys()),
                            range=list(genre_colors.values())
                        )
                    ),
                    tooltip=["content_genre", "unfinished_views"]
                )
                .properties(
                    height=400  # mesma altura para ambos
                )
            )
            st.altair_chart(chart_unf, use_container_width=True)
        else:
            st.info("CSV unfinished_by_genre.csv vazio ou mal formatado.")
    else:
        st.info("Arquivo unfinished_by_genre.csv não encontrado.")

























# === 5) SÉRIES TEMPORAIS: Faturamento ===
st.subheader("📈 Faturamento por mês")
path_month = os.path.join("transformed_data", "revenue_by_month.csv")
if os.path.exists(path_month):
    df_m = pd.read_csv(path_month, parse_dates=["month"])
    if not df_m.empty and {"month", "revenue"}.issubset(df_m.columns):
        df_m = df_m.sort_values("month")
        chart_m = (
            alt.Chart(df_m).mark_line(point=True)
            .encode(
                x=alt.X("month:T", title="Mês"),
                y=alt.Y("revenue:Q", title="Faturamento"),
                tooltip=["month", "revenue"]
            )
            .properties(width=700, height=300)
        )
        st.altair_chart(chart_m, use_container_width=True)
    else:
        st.info("CSV revenue_by_month.csv vazio ou mal formatado.")
else:
    st.info("Arquivo revenue_by_month.csv não encontrado.")

st.subheader("📈 Faturamento diário")
path_day = os.path.join("transformed_data", "revenue_by_day.csv")
if os.path.exists(path_day):
    df_d = pd.read_csv(path_day, parse_dates=["date"])
    if not df_d.empty and {"date", "revenue"}.issubset(df_d.columns):
        df_d = df_d.sort_values("date")
        chart_d = (
            alt.Chart(df_d).mark_line(point=True)
            .encode(
                x=alt.X("date:T", title="Data"),
                y=alt.Y("revenue:Q", title="Faturamento"),
                tooltip=["date", "revenue"]
            )
            .properties(width=700, height=300)
        )
        st.altair_chart(chart_d, use_container_width=True)
    else:
        st.info("CSV revenue_by_day.csv vazio ou mal formatado.")
else:
    st.info("Arquivo revenue_by_day.csv não encontrado.")
# ... lá em cima do seu script, depois de importar tudo ...

# === Bloco fixo na sidebar: Tempo médio do pipeline por nº de processos ===
with st.sidebar:
    st.subheader("⏱️ Pipeline: Tempo médio x Processos")

    runs_path = os.path.join("src", "transformed_data", "stage_metrics.csv")
    if os.path.exists(runs_path):
        # 1) carrega CSV sem cabeçalho
        df_runs = pd.read_csv(
            runs_path,
            names=["start_time", "stage", "processes", "duration_sec"],
            parse_dates=["start_time"],
            infer_datetime_format=True,
        )

        # 2) agrupa e tira média + contagem
        df_avg = (
            df_runs.groupby("processes", as_index=False)
                   .agg(media_segundos=("duration_sec", "mean"),
                        execucoes=("duration_sec", "count"))
                   .sort_values("processes")
        )
        # arredonda pra 2 casas
        df_avg["media_segundos"] = df_avg["media_segundos"].round(2)

        # 3) tabela simples
        st.table(df_avg.rename(columns={
            "processes": "Processos",
            "media_segundos": "Média (s)",
            "execucoes": "Execuções"
        }))

        # 4) gráfico com paleta vermelho→verde (valores altos vermelhos)
        chart_time = (
            alt.Chart(df_avg)
               .mark_bar()
               .encode(
                   x=alt.X("processes:O", title="Nº de processos"),
                   y=alt.Y("media_segundos:Q", title="Tempo médio (s)"),
                   color=alt.Color(
                       "media_segundos:Q",
                       title="Tempo médio (s)",
                       scale=alt.Scale(
                           scheme="redyellowgreen",
                           reverse=True
                       )
                   ),
                   tooltip=[
                       alt.Tooltip("processes:O", title="Processos"),
                       alt.Tooltip("media_segundos:Q", title="Média (s)", format=".2f"),
                       alt.Tooltip("execucoes:Q", title="Execuções"),
                   ]
               )
               .properties(width=250, height=250)
        )
        st.altair_chart(chart_time, use_container_width=True)

    else:
        st.info("Ainda não existem métricas gravadas em `stage_metrics.csv`.")
# === Botão para resetar o estado ===
if st.sidebar.button("🔄 Resetar Estado"):
    try:
        # Executa o script Python
        result = subprocess.run(
            ["python", os.path.join("src","reset_state.py")],
            capture_output=True,
            text=True,
            check=True
        )
        st.sidebar.success("✅ Estado resetado com sucesso!")
        # (Opcional) mostrar saída do script:
        # st.sidebar.text(result.stdout)
    except subprocess.CalledProcessError as e:
        st.sidebar.error(f"❌ Falha ao resetar:\n{e.stderr}")
    except Exception as e:
        st.sidebar.error(f"❌ Erro inesperado: {e}")
