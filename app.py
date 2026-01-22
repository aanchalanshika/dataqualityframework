import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime
from standardizer import standardize_data
from validator import validate_data
import plotly.graph_objects as go

st.set_page_config(
    page_title="Data Quality Framework",
    page_icon="📊",
    layout="wide"
)

# Enhanced CSS with animations
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&display=swap');
    
    * {
        font-family: 'Poppins', sans-serif;
    }
    
    /* Professional dark gradient background */
    .stApp {
        background: linear-gradient(135deg, #1e3a8a 0%, #312e81 50%, #1e293b 100%);
    }
    
    /* Fade in animation for main content */
    .main > div {
        animation: fadeInUp 0.8s ease-out;
    }
    
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    /* Title animation */
    h1 {
        color: white !important;
        text-align: center;
        animation: fadeInDown 1s ease-out;
    }
    
    @keyframes fadeInDown {
        from {
            opacity: 0;
            transform: translateY(-30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    /* Button animations */
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        font-size: 1.1rem;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    
    .stButton>button::before {
        content: "";
        position: absolute;
        top: 50%;
        left: 50%;
        width: 0;
        height: 0;
        border-radius: 50%;
        background: rgba(255, 255, 255, 0.2);
        transform: translate(-50%, -50%);
        transition: width 0.6s, height 0.6s;
    }
    
    .stButton>button:hover::before {
        width: 300px;
        height: 300px;
    }
    
    .stButton>button:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.6);
    }
    
    /* Metric cards with animations */
    [data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.1);
        padding: 2rem;
        border-radius: 16px;
        border: 2px solid rgba(255, 255, 255, 0.2);
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        animation: slideInLeft 0.6s ease-out;
        animation-fill-mode: both;
    }
    
    @keyframes slideInLeft {
        from {
            opacity: 0;
            transform: translateX(-50px);
        }
        to {
            opacity: 1;
            transform: translateX(0);
        }
    }
    
    [data-testid="stMetric"]:nth-child(1) { animation-delay: 0.1s; }
    [data-testid="stMetric"]:nth-child(2) { animation-delay: 0.2s; }
    [data-testid="stMetric"]:nth-child(3) { animation-delay: 0.3s; }
    [data-testid="stMetric"]:nth-child(4) { animation-delay: 0.4s; }
    
    [data-testid="stMetric"]:hover {
        transform: translateY(-8px) scale(1.05);
        background: rgba(255, 255, 255, 0.15);
        border-color: rgba(255, 255, 255, 0.4);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.5);
    }
    
    [data-testid="stMetricValue"] {
        font-size: 2.5rem;
        font-weight: 700;
        color: white !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: rgba(255, 255, 255, 0.9) !important;
        font-weight: 600;
        font-size: 1rem;
    }
    
    /* File uploader animation */
    [data-testid="stFileUploader"] {
        background: rgba(255, 255, 255, 0.08);
        padding: 2rem;
        border-radius: 16px;
        border: 2px dashed rgba(255, 255, 255, 0.3);
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
        animation: pulse 2s ease-in-out infinite;
    }
    
    @keyframes pulse {
        0%, 100% {
            border-color: rgba(255, 255, 255, 0.3);
        }
        50% {
            border-color: rgba(102, 126, 234, 0.6);
        }
    }
    
    [data-testid="stFileUploader"]:hover {
        background: rgba(255, 255, 255, 0.12);
        border-color: rgba(102, 126, 234, 0.8);
        transform: translateY(-2px);
        animation: none;
    }
    
    [data-testid="stFileUploader"] label {
        color: white !important;
        font-weight: 600;
    }
    
    /* Success/Info boxes */
    .stSuccess, .stInfo {
        border-radius: 12px;
        animation: slideInRight 0.5s ease-out;
        backdrop-filter: blur(10px);
    }
    
    @keyframes slideInRight {
        from {
            opacity: 0;
            transform: translateX(30px);
        }
        to {
            opacity: 1;
            transform: translateX(0);
        }
    }
    
    /* Expander animation */
    .streamlit-expanderHeader {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        transition: all 0.3s ease;
        color: white;
    }
    
    .streamlit-expanderHeader:hover {
        background: rgba(255, 255, 255, 0.15);
        transform: translateX(5px);
    }
    
    /* DataFrame animation */
    [data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        animation: zoomIn 0.5s ease-out;
    }
    
    @keyframes zoomIn {
        from {
            opacity: 0;
            transform: scale(0.95);
        }
        to {
            opacity: 1;
            transform: scale(1);
        }
    }
    
    /* Download buttons */
    .stDownloadButton>button {
        background: rgba(255, 255, 255, 0.1);
        color: white;
        border: 2px solid rgba(255, 255, 255, 0.3);
        border-radius: 10px;
        padding: 0.6rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stDownloadButton>button:hover {
        background: linear-gradient(135deg, #667eea, #764ba2);
        border-color: transparent;
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
    }
    
    /* Spinner customization */
    .stSpinner > div {
        border-color: #667eea !important;
        border-right-color: transparent !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 Data Quality Framework")
st.markdown("<p style='text-align: center; color: rgba(255,255,255,0.8); font-size: 1.1rem; margin-top: -1rem;'>Upload a CSV file to validate, standardize, and generate a data quality report.</p>", unsafe_allow_html=True)

with open("config/schema.json", "r", encoding="utf-8") as f:
    schema = json.load(f)

uploaded_file = st.file_uploader(
    "Upload your CSV file",
    type=["csv"]
)

if uploaded_file is None:
    st.info("👆 Please upload a CSV file to begin")
    st.stop()

df_raw = pd.read_csv(uploaded_file)

st.success(
    f"File loaded: {uploaded_file.name} "
    f"({len(df_raw)} rows, {len(df_raw.columns)} columns)"
)

with st.expander("📄 Preview Raw Data"):
    st.dataframe(df_raw.head(10), use_container_width=True)

if st.button("🚀 Run Quality Check"):

    with st.spinner("Processing data..."):

        df_standardized = standardize_data(df_raw, schema)
        validation_results = validate_data(df_standardized, schema)

        def flag_invalid_rows(df, schema):
            errors = [[] for _ in range(len(df))]
            is_valid = [True] * len(df)

            for col, spec in schema.get("columns", {}).items():
                if col not in df.columns:
                    for i in range(len(df)):
                        errors[i].append(f"missing column: {col}")
                        is_valid[i] = False
                    continue

                if spec.get("required"):
                    for i, val in enumerate(df[col].isna()):
                        if val:
                            errors[i].append(f"{col} is required")
                            is_valid[i] = False

            flagged = df.copy()
            flagged["is_valid"] = is_valid
            flagged["errors"] = ["; ".join(e) for e in errors]
            return flagged

        df_flagged = flag_invalid_rows(df_standardized, schema)

        total_rows = len(df_flagged)
        valid_rows = df_flagged["is_valid"].sum()
        invalid_rows = total_rows - valid_rows

        stats = validation_results.get("statistics", {})
        passed = stats.get("successful_expectations", 0)
        total_exp = stats.get("evaluated_expectations", 0)

        st.success("✅ Quality check completed")

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.metric("📊 Total Rows", total_rows)

        with c2:
            st.metric("✅ Valid Rows", valid_rows)

        with c3:
            st.metric("❌ Invalid Rows", invalid_rows)

        with c4:
            st.metric("🎯 Validation Score", f"{passed}/{total_exp}")

        c5, c6 = st.columns(2)

        with c5:
            pie = go.Figure(go.Pie(
                labels=["Valid", "Invalid"],
                values=[valid_rows, invalid_rows],
                hole=0.4,
                marker=dict(colors=['#10b981', '#ef4444'])
            ))
            pie.update_layout(
                title=dict(text="Valid vs Invalid Rows", font=dict(color='white', size=18)),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                showlegend=True,
                legend=dict(font=dict(color='white')),
                margin=dict(t=50, b=20, l=20, r=20)
            )
            st.plotly_chart(pie, use_container_width=True)

        with c6:
            bar = go.Figure(go.Bar(
                x=["Passed", "Failed"],
                y=[passed, total_exp - passed],
                marker=dict(color=['#10b981', '#ef4444'])
            ))
            bar.update_layout(
                title=dict(text="Validation Results", font=dict(color='white', size=18)),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                xaxis=dict(color='white', showgrid=False),
                yaxis=dict(color='white', showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
                margin=dict(t=50, b=50, l=50, r=20)
            )
            st.plotly_chart(bar, use_container_width=True)

        if invalid_rows > 0:
            st.markdown("### ❌ Invalid Records")
            invalid_df = df_flagged[~df_flagged["is_valid"]]
            st.dataframe(invalid_df, use_container_width=True)
        else:
            st.success("🎉 All records are valid!")

        st.markdown("### 💾 Downloads")

        st.download_button(
            "📥 Download Cleaned CSV",
            df_standardized.to_csv(index=False).encode("utf-8"),
            file_name="cleaned_data.csv",
            mime="text/csv"
        )

        if invalid_rows > 0:
            st.download_button(
                "📥 Download Invalid Rows",
                invalid_df.to_csv(index=False).encode("utf-8"),
                file_name="invalid_rows.csv",
                mime="text/csv"
            )

        os.makedirs("reports", exist_ok=True)
        report_name = f"data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        html_report = f"""
        <html>
        <head><title>Data Quality Report</title></head>
        <body style="font-family:Arial;">
            <h1>Data Quality Report</h1>
            <p><b>Source:</b> {uploaded_file.name}</p>
            <p><b>Total Rows:</b> {total_rows}</p>
            <p><b>Valid Rows:</b> {valid_rows}</p>
            <p><b>Invalid Rows:</b> {invalid_rows}</p>
            <h2>Invalid Records</h2>
            {invalid_df.to_html(index=False) if invalid_rows > 0 else "<p>No invalid rows</p>"}
        </body>
        </html>
        """

        with open(os.path.join("reports", report_name), "w", encoding="utf-8") as f:
            f.write(html_report)

        st.download_button(
            "📥 Download HTML Report",
            html_report.encode("utf-8"),
            file_name=report_name,
            mime="text/html"
        )

        st.success("✅ HTML report generated successfully")

st.markdown("---")
st.caption("Built with ❤️ using Streamlit • Data Quality Framework")
