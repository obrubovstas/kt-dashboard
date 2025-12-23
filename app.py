import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="KT dashboard", layout="wide")

@st.cache_resource
def get_engine():
    return create_engine(
        st.secrets["DATABASE_URL"],
        pool_pre_ping=True
    )

engine = get_engine()

def read_csv_ru(f):
    return pd.read_csv(f, sep=";", encoding="utf-8-sig", engine="python")

# ---------- LOADERS ----------

def pick_col(df, candidates):
    cols = {c.strip(): c for c in df.columns}  # map stripped->original
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    # fallback: try case-insensitive contains
    lowered = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in lowered:
            return lowered[key]
    raise KeyError(f"Не найдена ни одна из колонок: {candidates}. Фактические колонки: {list(df.columns)}")

def load_clicks(file):
    st.write("🧩 start load_clicks")  # маркер, должен появиться мгновенно

    df_iter = pd.read_csv(
        file,
        sep=";",
        encoding="utf-8-sig",
        engine="python",
        chunksize=200_000
    )

    st.write("🧩 csv iterator created")  # тоже должен появиться быстро

    total_rows = 0
    chunks_done = 0

    for chunk in df_iter:
        chunks_done += 1

        time_col = pick_col(chunk, ["Время клика", "Дата и время"])
        subid_col = pick_col(chunk, ["Subid", "SubId", "subid", "SUBID"])

        chunk["day"] = pd.to_datetime(chunk[time_col], errors="coerce").dt.date
        chunk["subid"] = chunk[subid_col].astype(str)

        agg = (
            chunk.dropna(subset=["day", "subid"])
                 .groupby(["day", "subid"])
                 .size()
                 .reset_index(name="clicks")
        )

        if not agg.empty:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                    insert into fact_clicks_daily(day, subid, clicks)
                    values (:day, :subid, :clicks)
                    on conflict (day, subid)
                    do update set clicks = fact_clicks_daily.clicks + excluded.clicks
                    """),
                    agg.to_dict("records")
                )

        total_rows += len(chunk)
        st.write(f"⬆️ chunk #{chunks_done}: обработано строк кликов: {total_rows:,}")

    st.write(f"✅ clicks загружены, всего строк: {total_rows:,}")

def load_conversions(file):
    df = read_csv_ru(file)
    df["subid"] = df["Subid"]

    df["day_lead"] = pd.to_datetime(df["Время конверсии"], errors="coerce").dt.date

    sale_time = df["Время продажи"].where(
        df["Время продажи"].notna() & (df["Время продажи"] != ""),
        df["Время конверсии"]
    )
    df["day_sale"] = pd.to_datetime(sale_time, errors="coerce").dt.date

    leads = (
        df[df["Ориг. статус"].str.lower() == "lead"]
        .dropna(subset=["day_lead", "subid"])
        .groupby(["day_lead", "subid"])
        .size()
        .reset_index(name="leads")
        .rename(columns={"day_lead": "day"})
    )

    sales = (
        df[df["Ориг. статус"].str.lower() == "sale"]
        .dropna(subset=["day_sale", "subid"])
        .groupby(["day_sale", "subid"])
        .size()
        .reset_index(name="sales")
        .rename(columns={"day_sale": "day"})
    )

    merged = (
        pd.merge(leads, sales, on=["day", "subid"], how="outer")
          .fillna(0)
          .astype({"leads": int, "sales": int})
    )

    with engine.begin() as conn:
        conn.execute(
            text("""
            insert into fact_conversions_daily(day, subid, leads, sales)
            values (:day, :subid, :leads, :sales)
            on conflict (day, subid)
            do update set
              leads = excluded.leads,
              sales = excluded.sales
            """),
            merged.to_dict("records")
        )

# ---------- UI ----------

st.title("📊 KT dashboard")
st.caption("build: 2025-12-23 v2 chunks")

with st.sidebar:
    st.header("Загрузка CSV")
    clicks = st.file_uploader("click.csv", type="csv")
    conv = st.file_uploader("conv.csv", type="csv")

if st.button("Загрузить в БД", type="primary"):
    with st.spinner("Загружаю данные в базу..."):
        if clicks:
            st.write("📥 Загружаю clicks...")
            load_clicks(clicks)
            st.write("✅ clicks загружены")

        if conv:
            st.write("📥 Загружаю conversions...")
            load_conversions(conv)
            st.write("✅ conversions загружены")

    st.success("🎉 Данные успешно загружены в БД")

# ---------- DASHBOARD ----------

df = pd.read_sql("""
select
  c.day,
  c.subid,
  c.clicks,
  coalesce(v.leads,0) as leads,
  coalesce(v.sales,0) as sales
from fact_clicks_daily c
left join fact_conversions_daily v
  on v.day = c.day and v.subid = c.subid
order by c.day;
""", engine)

if df.empty:
    st.info("Загрузи CSV файлы")
    st.stop()

last_day = df["day"].max()
prev_day = df[df["day"] < last_day]["day"].max()

k1, k2, k3 = st.columns(3)
k1.metric("Продажи", int(df[df.day == last_day].sales.sum()))
k2.metric("Регистрации", int(df[df.day == last_day].leads.sum()))
k3.metric("Клики", int(df[df.day == last_day].clicks.sum()))

st.subheader("📈 Продажи по дням")
st.line_chart(df.groupby("day")["sales"].sum())

if prev_day:
    st.subheader("🚀 ТОП Subid по росту продаж")

    today = df[df.day == last_day].groupby("subid")["sales"].sum()
    yday = df[df.day == prev_day].groupby("subid")["sales"].sum()

    growth = (
        today.subtract(yday, fill_value=0)
             .sort_values(ascending=False)
             .head(20)
             .reset_index(name="Δ sales")
    )

    st.dataframe(growth, use_container_width=True)
