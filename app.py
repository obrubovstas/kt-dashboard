import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="KT dashboard", layout="wide")

# ---------------- DB ----------------
@st.cache_resource
def get_engine():
    return create_engine(
        st.secrets["DATABASE_URL"],
        pool_pre_ping=True,
        future=True
    )

engine = get_engine()

# ---------------- Helpers ----------------
def read_csv_ru(f):
    # utf-8-sig лечит BOM в заголовках
    return pd.read_csv(f, sep=";", encoding="utf-8-sig", engine="python")

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    # 1) точное совпадение после strip
    stripped_map = {c.strip(): c for c in df.columns}
    for cand in candidates:
        if cand in stripped_map:
            return stripped_map[cand]

    # 2) case-insensitive совпадение
    lowered = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in lowered:
            return lowered[key]

    raise KeyError(
        f"Не найдена колонка из списка {candidates}. "
        f"Фактические колонки: {list(df.columns)}"
    )

def copy_df_to_table(conn, df: pd.DataFrame, table: str):
    """
    COPY df -> table (Postgres) через psycopg2 copy_expert.
    conn: SQLAlchemy Connection (внутри engine.begin()).
    """
    raw = conn.connection  # psycopg2 connection
    cur = raw.cursor()

    buf = io.StringIO()
    # Без header, иначе COPY будет пытаться вставить заголовок как данные
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols = ",".join(df.columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)"

    cur.copy_expert(sql, buf)
    cur.close()

# ---------------- Loaders ----------------
def load_clicks(file):
    st.write("🧩 start_load_clicks")

    # Читаем кусками — чтобы не убивать память на Streamlit Cloud
    df_iter = pd.read_csv(
        file,
        sep=";",
        encoding="utf-8-sig",
        engine="python",
        chunksize=200_000
    )
    st.write("🧩 csv iterator created")

    chunks_done = 0
    total_rows = 0
    progress = st.progress(0)

    with engine.begin() as conn:
        for chunk in df_iter:
            chunks_done += 1

            # выбираем колонки
            time_col = pick_col(chunk, ["Время клика", "Дата и время", "Click time", "Click Time"])
            subid_col = pick_col(chunk, ["Subid", "SubId", "subid", "SUBID"])

            chunk["day"] = pd.to_datetime(chunk[time_col], errors="coerce").dt.date
            chunk["subid"] = chunk[subid_col].astype(str)

            agg = (
                chunk.dropna(subset=["day", "subid"])
                     .groupby(["day", "subid"])
                     .size()
                     .reset_index(name="clicks")
            )

            total_rows += len(chunk)
            st.write(f"⬆️ chunk #{chunks_done}: прочитал {total_rows:,} строк, аггрегировал {len(agg):,}…")

            if agg.empty:
                continue

            # ---- DB pipeline: TRUNCATE staging -> COPY -> MERGE ----
            st.write("🧪 before TRUNCATE staging")
            conn.execute(text("truncate staging_clicks_daily;"))
            st.write("🧪 after TRUNCATE staging")

            st.write("🧪 before COPY to staging")
            copy_df_to_table(conn, agg[["day", "subid", "clicks"]], "staging_clicks_daily")
            st.write("🧪 after COPY to staging")

            st.write("🧪 before MERGE to fact")
            conn.execute(text("""
                insert into fact_clicks_daily(day, subid, clicks)
                select day, subid, clicks
                from staging_clicks_daily
                on conflict (day, subid)
                do update set clicks = fact_clicks_daily.clicks + excluded.clicks;
            """))
            st.write("🧪 after MERGE to fact")

            progress.progress(min(0.99, chunks_done / 20))

    progress.progress(1.0)
    st.write(f"🎉 clicks загружены, всего исходных строк: {total_rows:,}")

def load_conversions(file):
    st.write("🧩 start_load_conversions")

    df = read_csv_ru(file)

    subid_col = pick_col(df, ["Subid", "SubId", "subid", "SUBID"])
    status_col = pick_col(df, ["Ориг. статус", "Orig. status", "Orig status", "Status"])
    conv_time_col = pick_col(df, ["Время конверсии", "Conversion time"])

    sale_time_col = None
    for cand in ["Время продажи", "Sale time"]:
        try:
            sale_time_col = pick_col(df, [cand])
            break
        except Exception:
            pass

    df["subid"] = df[subid_col].astype(str)
    df["_status"] = df[status_col].astype(str).str.lower()

    df["day_lead"] = pd.to_datetime(df[conv_time_col], errors="coerce").dt.date

    if sale_time_col:
        sale_time = df[sale_time_col].where(
            df[sale_time_col].notna() & (df[sale_time_col].astype(str) != ""),
            df[conv_time_col]
        )
    else:
        sale_time = df[conv_time_col]

    df["day_sale"] = pd.to_datetime(sale_time, errors="coerce").dt.date

    leads = (
        df[df["_status"] == "lead"]
        .dropna(subset=["day_lead", "subid"])
        .groupby(["day_lead", "subid"])
        .size()
        .reset_index(name="leads")
        .rename(columns={"day_lead": "day"})
    )

    sales = (
        df[df["_status"] == "sale"]
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

    st.write(f"🧪 conversions aggregated rows: {len(merged):,}")

    # 🔽 ВАЖНО: этот with НА ТОМ ЖЕ УРОВНЕ, что и merged
    with engine.begin() as conn:
        st.write("🧪 before DELETE conv staging")
        conn.execute(text("delete from staging_conversions_daily;"))
        st.write("🧪 after DELETE conv staging")

        st.write("🧪 before COPY conv to staging")
        copy_df_to_table(
            conn,
            merged[["day", "subid", "leads", "sales"]],
            "staging_conversions_daily"
        )
        st.write("🧪 after COPY conv to staging")

        # 🔍 ЛОГ №1 — перед UPDATE
        st.write("🧪 before UPDATE fact")
        
        # 1) UPDATE существующих строк
        conn.execute(text("""
            update fact_conversions_daily f
            set
              leads = s.leads,
              sales = s.sales
            from staging_conversions_daily s
            where f.day = s.day and f.subid = s.subid;
        """))
        
        # 🔍 ЛОГ №2 — после UPDATE, перед INSERT
        st.write("🧪 after UPDATE fact, before INSERT new")
        
        # 2) INSERT только новых строк
        conn.execute(text("""
            insert into fact_conversions_daily(day, subid, leads, sales)
            select s.day, s.subid, s.leads, s.sales
            from staging_conversions_daily s
            left join fact_conversions_daily f
              on f.day = s.day and f.subid = s.subid
            where f.subid is null;
        """))
        
        # 🔍 ЛОГ №3 — после INSERT
        st.write("🧪 after INSERT new")


        st.write("🧪 after MERGE conv to fact")

    st.write("🎉 conversions загружены")

# ---------------- UI ----------------
st.title("📊 KT dashboard")
st.caption("build: 2025-12-23 v3 copy")

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

# ---------------- DASHBOARD ----------------
df = pd.read_sql("""
with keys as (
    select day, subid from fact_clicks_daily
    union
    select day, subid from fact_conversions_daily
)
select
    k.day,
    k.subid,
    coalesce(c.clicks, 0) as clicks,
    coalesce(v.leads, 0) as leads,
    coalesce(v.sales, 0) as sales
from keys k
left join fact_clicks_daily c
  on c.day = k.day and c.subid = k.subid
left join fact_conversions_daily v
  on v.day = k.day and v.subid = k.subid
order by k.day;
""", engine)

if df.empty:
    st.info("Загрузи CSV файлы — появится дашборд.")
    st.stop()

import datetime as dt

today = dt.date.today()
last_day = today - dt.timedelta(days=1)  # вчера
prev_day = last_day - dt.timedelta(days=1)  # позавчера

st.write("Дни в базе:", sorted(df["day"].unique()))

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
else:
    st.warning("Пока только один день в базе — для сравнения нужен минимум 2 дня.")
