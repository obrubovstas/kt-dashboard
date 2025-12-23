import io
import datetime as dt

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
        future=True,
    )


engine = get_engine()


# ---------------- Helpers ----------------
def read_csv_ru(f):
    # utf-8-sig лечит BOM в заголовках
    return pd.read_csv(f, sep=";", encoding="utf-8-sig", engine="python")


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    stripped_map = {c.strip(): c for c in df.columns}
    for cand in candidates:
        if cand in stripped_map:
            return stripped_map[cand]

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
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols = ",".join(df.columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)"

    cur.copy_expert(sql, buf)
    cur.close()

def fmt_pct_cell(val):
    # текст в ячейке
    if val is None or pd.isna(val):
        return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"

def style_pct_color(val):
    # цвет текста
    if val is None or pd.isna(val):
        return ""
    if val > 0:
        return "color: #22c55e; font-weight: 700;"
    if val < 0:
        return "color: #ef4444; font-weight: 700;"
    return "color: #a3a3a3;"

def pct_change(curr: float, prev: float):
    if prev is None or prev == 0:
        return None
    return (curr - prev) / prev * 100.0


def metric_with_pct(label: str, curr: int, prev: int):
    d = pct_change(curr, prev)
    if d is None:
        st.metric(label, curr, delta="—", delta_color="off")
    else:
        st.metric(label, curr, delta=float(round(d, 2)), delta_color="normal")


# ---------------- Schema bootstrap (safe) ----------------
def ensure_schema():
    # Таблицы фактов у тебя уже есть, но на всякий случай.
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                create table if not exists fact_clicks_daily (
                  day date not null,
                  subid text not null,
                  clicks bigint not null,
                  primary key (day, subid)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                create table if not exists fact_conversions_daily (
                  day date not null,
                  subid text not null,
                  leads bigint not null,
                  sales bigint not null,
                  primary key (day, subid)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                create table if not exists staging_clicks_daily (
                  day date not null,
                  subid text not null,
                  clicks bigint not null
                );
                """
            )
        )
        conn.execute(
            text(
                """
                create table if not exists dim_subid (
                  subid text primary key,
                  offer text,
                  country_flag text,
                  os text,
                  sub_id_2 text,
                  campaign text,
                  sub_id_1 text,
                  updated_at timestamptz default now()
                );
                """
            )
        )
        conn.execute(text("create index if not exists idx_dim_subid_sub2 on dim_subid(sub_id_2);"))
        conn.execute(text("create index if not exists idx_dim_subid_campaign on dim_subid(campaign);"))
        conn.execute(text("create index if not exists idx_dim_subid_offer on dim_subid(offer);"))


ensure_schema()


# ---------------- Loaders ----------------
def load_clicks(file):
    st.write("🧩 start_load_clicks")

    df_iter = pd.read_csv(
        file,
        sep=";",
        encoding="utf-8-sig",
        engine="python",
        chunksize=200_000,
    )
    st.write("🧩 csv iterator created")

    chunks_done = 0
    total_rows = 0
    progress = st.progress(0)

    with engine.begin() as conn:
        for chunk in df_iter:
            chunks_done += 1

            time_col = pick_col(chunk, ["Время клика", "Дата и время", "Click time", "Click Time"])
            subid_col = pick_col(chunk, ["Subid", "SubId", "subid", "SUBID"])

            # доп. поля из click.csv
            offer_col = pick_col(chunk, ["Оффер", "Offer"])
            flag_col = pick_col(chunk, ["Флаг страны", "Country flag", "Flag"])
            os_col = pick_col(chunk, ["ОС", "OS"])
            sub2_col = pick_col(chunk, ["Sub ID 2", "Subid 2", "Sub2", "Sub ID2"])
            camp_col = pick_col(chunk, ["Кампания", "Campaign"])
            sub1_col = pick_col(chunk, ["Sub ID 1", "Subid 1", "Sub1", "Sub ID1"])

            chunk["day"] = pd.to_datetime(chunk[time_col], errors="coerce").dt.date
            chunk["subid"] = chunk[subid_col].astype(str)

            # -------- DIM загрузка (по subid) через TEMP staging --------
            dim = chunk[[subid_col, offer_col, flag_col, os_col, sub2_col, camp_col, sub1_col]].copy()
            dim.columns = ["subid", "offer", "country_flag", "os", "sub_id_2", "campaign", "sub_id_1"]
            dim["subid"] = dim["subid"].astype(str)

            # выкидываем пустые subid
            dim = dim[dim["subid"].notna() & (dim["subid"].astype(str).str.len() > 0)]
            # одна строка на subid в рамках чанка (последняя)
            dim = dim.drop_duplicates(subset=["subid"], keep="last")

            # TEMP table, без TRUNCATE/DELETE постоянных таблиц
            conn.execute(text("drop table if exists staging_dim_subid_tmp;"))
            conn.execute(
                text(
                    """
                    create temporary table staging_dim_subid_tmp (
                      subid text,
                      offer text,
                      country_flag text,
                      os text,
                      sub_id_2 text,
                      campaign text,
                      sub_id_1 text
                    ) on commit drop;
                    """
                )
            )
            copy_df_to_table(conn, dim[["subid", "offer", "country_flag", "os", "sub_id_2", "campaign", "sub_id_1"]],
                             "staging_dim_subid_tmp")

            # UPDATE существующих (берём только непустые значения из staging)
            conn.execute(
                text(
                    """
                    update dim_subid d
                    set
                      offer = coalesce(nullif(s.offer,''), d.offer),
                      country_flag = coalesce(nullif(s.country_flag,''), d.country_flag),
                      os = coalesce(nullif(s.os,''), d.os),
                      sub_id_2 = coalesce(nullif(s.sub_id_2,''), d.sub_id_2),
                      campaign = coalesce(nullif(s.campaign,''), d.campaign),
                      sub_id_1 = coalesce(nullif(s.sub_id_1,''), d.sub_id_1),
                      updated_at = now()
                    from staging_dim_subid_tmp s
                    where d.subid = s.subid;
                    """
                )
            )

            # INSERT новых
            conn.execute(
                text(
                    """
                    insert into dim_subid(subid, offer, country_flag, os, sub_id_2, campaign, sub_id_1)
                    select s.subid, s.offer, s.country_flag, s.os, s.sub_id_2, s.campaign, s.sub_id_1
                    from staging_dim_subid_tmp s
                    left join dim_subid d on d.subid = s.subid
                    where d.subid is null;
                    """
                )
            )

            # -------- FACT clicks --------
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

            # staging_clicks_daily (перманентная) — можно TRUNCATE (у тебя direct connection)
            conn.execute(text("truncate staging_clicks_daily;"))
            copy_df_to_table(conn, agg[["day", "subid", "clicks"]], "staging_clicks_daily")

            conn.execute(
                text(
                    """
                    insert into fact_clicks_daily(day, subid, clicks)
                    select day, subid, clicks
                    from staging_clicks_daily
                    on conflict (day, subid)
                    do update set clicks = fact_clicks_daily.clicks + excluded.clicks;
                    """
                )
            )

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
            df[conv_time_col],
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

    with engine.begin() as conn:
        # TEMP staging (не трогаем постоянные таблицы truncate/delete)
        conn.execute(text("drop table if exists staging_conversions_tmp;"))
        conn.execute(
            text(
                """
                create temporary table staging_conversions_tmp (
                  day date,
                  subid text,
                  leads bigint,
                  sales bigint
                ) on commit drop;
                """
            )
        )

        copy_df_to_table(conn, merged[["day", "subid", "leads", "sales"]], "staging_conversions_tmp")

        # UPDATE существующих
        conn.execute(
            text(
                """
                update fact_conversions_daily f
                set
                  leads = s.leads,
                  sales = s.sales
                from staging_conversions_tmp s
                where f.day = s.day and f.subid = s.subid;
                """
            )
        )

        # INSERT новых
        conn.execute(
            text(
                """
                insert into fact_conversions_daily(day, subid, leads, sales)
                select s.day, s.subid, s.leads, s.sales
                from staging_conversions_tmp s
                left join fact_conversions_daily f
                  on f.day = s.day and f.subid = s.subid
                where f.subid is null;
                """
            )
        )

    st.write("🎉 conversions загружены")


# ---------------- UI ----------------
st.title("📊 KT dashboard")
st.caption("build: 2025-12-23 v4 dims + gainers")

with st.sidebar:
    st.header("Загрузка CSV")
    clicks_file = st.file_uploader("click.csv", type="csv")
    conv_file = st.file_uploader("conv.csv", type="csv")

    if st.button("Загрузить в БД", type="primary"):
        with st.spinner("Загружаю данные в базу..."):
            if clicks_file:
                st.write("📥 Загружаю clicks...")
                load_clicks(clicks_file)
                st.write("✅ clicks загружены")

            if conv_file:
                st.write("📥 Загружаю conversions...")
                load_conversions(conv_file)
                st.write("✅ conversions загружены")

        st.success("🎉 Данные успешно загружены в БД")


# ---------------- Data for dashboard ----------------
df = pd.read_sql(
    """
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
      coalesce(v.sales, 0) as sales,
      d.offer,
      d.country_flag,
      d.os,
      d.sub_id_2,
      d.campaign,
      d.sub_id_1
    from keys k
    left join fact_clicks_daily c
      on c.day = k.day and c.subid = k.subid
    left join fact_conversions_daily v
      on v.day = k.day and v.subid = k.subid
    left join dim_subid d
      on d.subid = k.subid
    order by k.day;
    """,
    engine,
)

if df.empty:
    st.info("Загрузи CSV файлы — появится дашборд.")
    st.stop()

# Нормализация разрезов
df["sub_id_2"] = df["sub_id_2"].fillna("").astype(str).str.strip()
df["sub2_norm"] = df["sub_id_2"].replace({"": "Organic"})

df["campaign"] = df["campaign"].fillna("").astype(str)
df["campaign_short"] = df["campaign"].str.split("[", n=1).str[0].str.strip()

df["offer"] = df["offer"].fillna("").astype(str).str.strip()

# Периоды: вчера / позавчера
today = dt.date.today()
yday = today - dt.timedelta(days=1)
pday = today - dt.timedelta(days=2)

df_y = df[df["day"] == yday].copy()
df_p = df[df["day"] == pday].copy()

y_clicks = int(df_y["clicks"].sum())
p_clicks = int(df_p["clicks"].sum())

y_leads = int(df_y["leads"].sum())
p_leads = int(df_p["leads"].sum())

y_sales = int(df_y["sales"].sum())
p_sales = int(df_p["sales"].sum())

# KPI
k1, k2, k3 = st.columns(3)
with k1:
    metric_with_pct("Инсталлы", y_clicks, p_clicks)  # как ты просил: клики = инсталлы
with k2:
    metric_with_pct("Регистрации", y_leads, p_leads)
with k3:
    metric_with_pct("Продажи", y_sales, p_sales)

# График продаж по дням (оставляем)
st.subheader("📈 Продажи по дням")
st.line_chart(df.groupby("day")["sales"].sum())

# --- Top 5 Sub ID 2 by Sales (exclude Organic) ---
st.subheader("🏆 Топ 5 Sub ID 2 по продажам (вчера)")

df_y_non_org = df_y[df_y["sub2_norm"] != "Organic"].copy()
df_p_non_org = df_p[df_p["sub2_norm"] != "Organic"].copy()

top_sub2_y = df_y_non_org.groupby("sub2_norm")["sales"].sum().sort_values(ascending=False).head(5)
top_sub2_p = df_p_non_org.groupby("sub2_norm")["sales"].sum()

rows = []
for sub2, s_y in top_sub2_y.items():
    s_p = float(top_sub2_p.get(sub2, 0))
    ch = pct_change(float(s_y), float(s_p))
    rows.append(
        {
            "Sub ID 2": sub2,
            "Sales (yday)": int(s_y),
            "Δ% vs prev": None if ch is None else round(ch, 2),
        }
    )
df_tbl = pd.DataFrame(rows)

# форматируем отображение процента
df_tbl["Δ% vs prev"] = df_tbl["Δ% vs prev"].apply(lambda x: None if x is None else float(x))

sty = (
    df_tbl.style
    .format({"Δ% vs prev": fmt_pct_cell})
    .applymap(style_pct_color, subset=["Δ% vs prev"])
)

st.dataframe(sty, use_container_width=True)


# --- Top 5 Campaign by Sales (campaign_short) ---
st.subheader("🏆 Топ 5 Кампания по продажам (вчера)")

top_c_y = df_y.groupby("campaign_short")["sales"].sum().sort_values(ascending=False).head(5)
top_c_p = df_p.groupby("campaign_short")["sales"].sum()

rows = []
for camp, s_y in top_c_y.items():
    s_p = float(top_c_p.get(camp, 0))
    ch = pct_change(float(s_y), float(s_p))
    rows.append(
        {
            "Campaign": camp,
            "Sales (yday)": int(s_y),
            "Δ% vs prev": None if ch is None else round(ch, 2),
        }
    )
df_tbl = pd.DataFrame(rows)
df_tbl["Δ% vs prev"] = df_tbl["Δ% vs prev"].apply(lambda x: None if x is None else float(x))

sty = (
    df_tbl.style
    .format({"Δ% vs prev": fmt_pct_cell})
    .applymap(style_pct_color, subset=["Δ% vs prev"])
)

st.dataframe(sty, use_container_width=True)


# --- Gain helpers ---
def gain_table(group_col: str, metric_col: str, title: str, top_n: int = 10, exclude_organic: bool = False):
    st.subheader(title)

    a = df_y.copy()
    b = df_p.copy()

    if exclude_organic and group_col == "sub2_norm":
        a = a[a["sub2_norm"] != "Organic"]
        b = b[b["sub2_norm"] != "Organic"]

    y = a.groupby(group_col)[metric_col].sum()
    p = b.groupby(group_col)[metric_col].sum()

    out = (y.subtract(p, fill_value=0)).sort_values(ascending=False)
    out = out[out > 0].head(top_n)

    rows = []
    for key, delta in out.items():
        rows.append(
            {
                group_col: key,
                f"{metric_col} (yday)": int(y.get(key, 0)),
                f"{metric_col} (prev)": int(p.get(key, 0)),
                f"Δ {metric_col}": int(delta),
            }
        )

    st.dataframe(pd.DataFrame(rows), use_container_width=True)


# Traffic gainers (clicks)
gain_table("sub2_norm", "clicks", "📈 Top 10 Sub ID 2 Traffic Gainers (клики)", exclude_organic=True)
gain_table("campaign_short", "clicks", "📈 Top 10 Кампания Traffic Gainers (клики)")

# Sales gainers
gain_table("sub2_norm", "sales", "💰 Top 10 Sub ID 2 Sales Gainers (продажи)", exclude_organic=True)
gain_table("campaign_short", "sales", "💰 Top 10 Кампания Sales Gainers (продажи)")
gain_table("offer", "sales", "💰 Top 10 Оффер Sales Gainers (продажи)")
