"""
Customer Churn Prediction - Streamlit app
SIC graduation project, Task 4 (warehouse) + bonus ML stage.

Reads dim_customer.csv, the Hive warehouse table exported from the pipeline,
trains a random forest once per session and serves it behind three tabs.

Run:  streamlit run app.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, roc_curve, confusion_matrix

# --------------------------------------------------------------------------
# page config + palette (same colours as the notebook: blue = stayed,
# orange = churned, never swapped)
# --------------------------------------------------------------------------
st.set_page_config(page_title="Churn Prediction", page_icon="📉",
                   layout="wide", initial_sidebar_state="expanded")

STAYED, CHURNED = "#3B6EA8", "#C4622D"
GRID, INK, MUTED = "#E3E6EA", "#262626", "#6B7280"

plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.edgecolor": GRID, "axes.labelcolor": INK, "text.color": INK,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 10, "figure.dpi": 110,
})

st.markdown("""
<style>
  .block-container {padding-top: 2.2rem; max-width: 1250px;}
  [data-testid="stMetricValue"] {font-size: 1.9rem;}
  h1 {font-size: 2.0rem !important;}
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------------------------------
# data + model  (cached so they are computed once, not on every interaction)
# --------------------------------------------------------------------------
DROP = ["is_churned",                                  # target
        "churn_month",                                 # LEAKS THE ANSWER
        "cust_key", "customer_id",                     # identifiers
        "dw_start_date", "dw_end_date", "is_current"]  # SCD-2 bookkeeping

# resolve the CSV next to this file, not next to whatever the working
# directory happens to be - Streamlit Cloud runs from the repo root
DATA_FILE = Path(__file__).parent / "dim_customer.csv"


@st.cache_data
def load_data(path=DATA_FILE):
    # Hive writes nulls as the literal string NULL
    return pd.read_csv(path, na_values=["NULL"])


@st.cache_data
def build_features(df):
    d = df.copy()
    d["has_tickets"] = (d["total_tickets"] > 0).astype(int)
    y = d["is_churned"]
    X = d.drop(columns=DROP)
    X = pd.get_dummies(X, columns=["gender", "geography"], drop_first=True)
    return X.fillna(0), y


@st.cache_resource
def train(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42)

    rf = RandomForestClassifier(n_estimators=400, min_samples_leaf=5,
                                class_weight="balanced", random_state=42,
                                n_jobs=-1).fit(X_train, y_train)

    scaler = StandardScaler().fit(X_train)
    lr = LogisticRegression(max_iter=1000, class_weight="balanced")
    lr.fit(scaler.transform(X_train), y_train)

    return {
        "rf": rf, "lr": lr, "scaler": scaler,
        "X_train": X_train, "X_test": X_test,
        "y_train": y_train, "y_test": y_test,
        "proba_rf": rf.predict_proba(X_test)[:, 1],
        "proba_lr": lr.predict_proba(scaler.transform(X_test))[:, 1],
    }


try:
    df = load_data()
except FileNotFoundError:
    st.error("`dim_customer.csv` not found. Put it in the same folder as app.py.")
    st.stop()

X, y = build_features(df)
M = train(X, y)

rf, X_test, y_test = M["rf"], M["X_test"], M["y_test"]
proba_rf, proba_lr = M["proba_rf"], M["proba_lr"]
BASE = y_test.mean()


def draw_confusion(cm, figsize=(4.4, 3.9)):
    """Shared 2x2 confusion matrix plot."""
    fig, ax = plt.subplots(figsize=figsize)
    ax.imshow(cm, cmap="Blues", vmin=0, vmax=cm.max())
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{cm[i, j]:,}", ha="center", va="center",
                    fontsize=15, weight="bold",
                    color="white" if cm[i, j] > cm.max()*0.5 else INK)
    ax.set_xticks([0, 1], ["predicted\nstayed", "predicted\nchurned"])
    ax.set_yticks([0, 1], ["actually\nstayed", "actually\nchurned"])
    ax.grid(False)
    plt.tight_layout()
    return fig


# --------------------------------------------------------------------------
# sidebar
# --------------------------------------------------------------------------
with st.sidebar:
    st.header("Decision threshold")
    thr = st.slider(
        "Flag a customer when risk is at least", 0.05, 0.95, 0.35, 0.05,
        help="Lower it to catch more churners at the cost of more false alarms. "
             "0.5 is only sklearn's default, not a result.")

    flagged = (proba_rf >= thr).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_test, flagged).ravel()

    st.metric("Churners caught", f"{tp} of {tp + fn}", f"{tp/(tp+fn):.0%} recall")
    st.metric("Customers flagged", f"{flagged.sum():,}",
              f"{tp/max(flagged.sum(),1):.0%} precision", delta_color="off")
    st.metric("Missed churners", f"{fn}", delta_color="off")

    st.divider()
    st.caption(
        "Source: `churn_dw.dim_customer`, the Hive warehouse table produced by "
        "the pipeline (Sqoop / NiFi -> HDFS -> PySpark -> Hive). "
        "10,000 customers, 8,000 used for training, 2,000 held out for testing."
    )

st.title("Customer churn prediction")
st.caption("SIC graduation project - bonus ML stage, built on the Task 4 warehouse table")

tab_overview, tab_model, tab_score = st.tabs(
    ["Overview", "Model performance", "Score a customer"])


# ==========================================================================
# TAB 1 - overview of the data
# ==========================================================================
with tab_overview:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Customers", f"{len(df):,}")
    c2.metric("Churned", f"{int(df.is_churned.sum()):,}",
              f"{df.is_churned.mean():.1%} of base", delta_color="off")
    c3.metric("Countries", df.geography.nunique())
    c4.metric("Features used", X.shape[1])

    st.divider()
    left, right = st.columns([1, 1])

    with left:
        st.subheader("Churn rate by country")
        g = (df.groupby("geography")["is_churned"]
               .agg(["mean", "size"]).sort_values("mean"))

        fig, ax = plt.subplots(figsize=(5.6, 3.6))
        bars = ax.bar(g.index, g["mean"] * 100, color=CHURNED, width=0.55)
        for b, (rate, n) in zip(bars, g.values):
            ax.text(b.get_x() + b.get_width()/2, rate*100 + 0.7,
                    f"{rate*100:.1f}%\nn={int(n):,}", ha="center",
                    fontsize=9, color=MUTED)
        ax.set_ylabel("churn rate (%)")
        ax.set_ylim(0, g["mean"].max()*100 + 9)
        ax.set_axisbelow(True); ax.grid(axis="x", visible=False)
        plt.tight_layout(); st.pyplot(fig, width="stretch")

        st.markdown(
            "Germany churns at roughly **double** the rate of France and Spain "
            "on the same product. This is the clearest single signal in the data, "
            "and it is why the warehouse table is partitioned by `geography`.")

    with right:
        st.subheader("Distribution by outcome")
        feat = st.selectbox("Feature", ["age", "credit_score",
                                        "avg_monthly_balance", "tenure_months",
                                        "clv_ltv", "total_tickets"], index=0)

        fig, ax = plt.subplots(figsize=(5.6, 3.6))
        for lbl, colr, key in [("stayed", STAYED, 0), ("churned", CHURNED, 1)]:
            ax.hist(df.loc[df.is_churned == key, feat], bins=30, alpha=0.65,
                    color=colr, label=lbl, density=True)
        ax.set_yticks([]); ax.set_xlabel(feat)
        ax.legend(frameon=False, fontsize=9)
        ax.grid(axis="x", visible=False)
        plt.tight_layout(); st.pyplot(fig, width="stretch")

        st.markdown(
            "**Age** is the only feature with obvious separation - churners sit "
            "clearly to the right. Credit score and tenure barely separate at all, "
            "which means no single column solves this problem. Performance has to "
            "come from combining weak signals, which is what a tree ensemble does.")


# ==========================================================================
# TAB 2 - model performance
# ==========================================================================
with tab_model:
    auc_rf = roc_auc_score(y_test, proba_rf)
    auc_lr = roc_auc_score(y_test, proba_lr)

    c1, c2, c3 = st.columns(3)
    c1.metric("Random forest - ROC AUC", f"{auc_rf:.4f}")
    c2.metric("Logistic regression - ROC AUC", f"{auc_lr:.4f}",
              f"{auc_lr - auc_rf:+.4f} vs forest")
    c3.metric("Accuracy at 0.50", f"{(rf.predict(X_test) == y_test).mean():.1%}")

    st.divider()
    left, right = st.columns([1, 1])

    with left:
        st.subheader("ROC curve")
        fig, ax = plt.subplots(figsize=(5.2, 4.4))
        for nm, pb, colr in [("random forest", proba_rf, CHURNED),
                             ("logistic regression", proba_lr, STAYED)]:
            fpr, tpr, _ = roc_curve(y_test, pb)
            ax.plot(fpr, tpr, color=colr, linewidth=2,
                    label=f"{nm}  (AUC {roc_auc_score(y_test, pb):.3f})")
        ax.plot([0, 1], [0, 1], color=MUTED, linewidth=1, linestyle=":",
                label="random guessing")
        ax.set_xlabel("false positive rate"); ax.set_ylabel("true positive rate")
        ax.legend(frameon=False, fontsize=9, loc="lower right")
        ax.set_xlim(0, 1); ax.set_ylim(0, 1.02); ax.set_axisbelow(True)
        plt.tight_layout(); st.pyplot(fig, width="stretch")

    with right:
        st.subheader("Confusion matrix at 0.50")
        pred50 = (proba_rf >= 0.50).astype(int)
        tn50, fp50, fn50, tp50 = confusion_matrix(y_test, pred50).ravel()
        st.pyplot(draw_confusion(np.array([[tn50, fp50], [fn50, tp50]]),
                                 figsize=(5.2, 4.0)), width="stretch")
        st.caption(
            f"2,000 held-out customers. {tp50 + fn50} of them really churned: "
            f"the model caught **{tp50}** and missed **{fn50}**, while wrongly "
            f"flagging **{fp50}** of the {tn50 + fp50} who stayed. "
            "0.50 is sklearn's default cut-off, not a business decision - "
            "the sidebar slider moves it.")

    st.divider()
    left2, right2 = st.columns([1.5, 1])

    with left2:
        st.subheader("What predicts churn")
        imp = pd.Series(rf.feature_importances_, index=X.columns).sort_values()
        top = imp.tail(12)
        shades = plt.cm.Blues(np.linspace(0.42, 0.88, len(top)))

        fig, ax = plt.subplots(figsize=(5.6, 4.4))
        ax.barh(top.index, top.values, color=shades, height=0.68)
        for i, v in enumerate(top.values):
            ax.text(v + top.max()*0.015, i, f"{v:.3f}", va="center",
                    fontsize=8, color=MUTED)
        ax.set_xlabel("importance"); ax.set_xlim(0, top.max()*1.18)
        ax.grid(axis="y", visible=False); ax.set_axisbelow(True)
        plt.tight_layout(); st.pyplot(fig, width="stretch")

    with right2:
        st.markdown(
            "`avg_ticket_res_time_hrs`, `offer_acceptance_rate` and "
            "`total_tickets` exist **only because the ETL joined three separate "
            "source systems** into one row per customer.\n\n"
            "That join is the engineering contribution; the model is about twenty "
            "lines on top of it.")


# ==========================================================================
# TAB 3 - score one customer
# ==========================================================================
with tab_score:
    st.subheader("Score a single customer")
    st.caption("Enter details and the trained model returns a risk score - the "
               "same model, one row instead of 2,000.")

    c1, c2, c3 = st.columns(3)
    with c1:
        in_age = st.slider("Age", 18, 92, 45)
        in_geo = st.selectbox("Country", ["France", "Germany", "Spain"], index=1)
        in_gen = st.selectbox("Gender", ["Female", "Male"])
        in_ten = st.slider("Tenure (months)", 0, 120, 60)
    with c2:
        in_cs  = st.slider("Credit score", 350, 850, 650)
        in_np  = st.slider("Number of products", 1, 4, 2)
        in_bal = st.slider("Average monthly balance", 0, 260000, 120000, 1000)
        in_clv = st.slider("Lifetime value", 0, 50000, 9000, 100)
    with c3:
        in_tt  = st.slider("Total support tickets", 0, 15, 1)
        in_hs  = st.slider("High-severity tickets", 0, 8, 0)
        in_res = st.slider("Avg ticket resolution (hrs)", 0, 120, 24)
        in_or  = st.slider("Offers received", 0, 5, 3)
        in_oa  = st.slider("Offers accepted", 0, 5, 0)

    row = pd.DataFrame([{
        "age": in_age, "tenure_months": in_ten, "credit_score": in_cs,
        "num_products": in_np, "clv_ltv": in_clv,
        "avg_monthly_balance": in_bal,
        "avg_ticket_res_time_hrs": in_res if in_tt > 0 else 0,
        "total_tickets": in_tt, "high_severity_tickets": in_hs,
        "offers_received": in_or, "offers_accepted": in_oa,
        "offer_acceptance_rate": (in_oa / in_or) if in_or else 0,
        "has_tickets": int(in_tt > 0),
        "gender_Male": int(in_gen == "Male"),
        "geography_Germany": int(in_geo == "Germany"),
        "geography_Spain": int(in_geo == "Spain"),
    }])[X.columns]          # same column order the model was trained on

    risk = float(rf.predict_proba(row)[0, 1])

    st.divider()
    a, b = st.columns([1, 1.6])
    with a:
        st.metric("Churn risk", f"{risk:.1%}")
        if risk >= thr:
            st.error(f"FLAGGED - above the {thr:.2f} threshold. Contact this customer.")
        else:
            st.success(f"Not flagged - below the {thr:.2f} threshold.")
        st.caption(f"Base rate is {BASE:.1%}. This customer is "
                   f"**{risk/BASE:.1f}x** the average.")

    with b:
        fig, ax = plt.subplots(figsize=(6.4, 1.5))
        ax.barh([0], [risk], color=CHURNED if risk >= thr else STAYED, height=0.42)
        ax.axvline(thr, color=MUTED, linewidth=1.6, linestyle="--")
        ax.text(thr, 0.42, f" threshold {thr:.2f}", fontsize=9, color=MUTED)
        ax.set_xlim(0, 1); ax.set_ylim(-0.45, 0.75)
        ax.set_yticks([]); ax.set_xlabel("churn risk")
        ax.grid(axis="y", visible=False); ax.set_axisbelow(True)
        plt.tight_layout(); st.pyplot(fig, width="stretch")

    st.caption("Try it: set Country to Germany, Age to 50 and balance high - risk "
               "jumps. That matches both the feature-importance chart and the "
               "country chart on the Overview tab.")