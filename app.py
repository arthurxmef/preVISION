import streamlit as st
import pandas as pd
import re
from datetime import datetime
import io
import plotly.graph_objects as go
import plotly.express as px
from functools import reduce

# --- Page Configuration ---
st.set_page_config(page_title="SKU Sales Merger", layout="wide")

# --- Helper Functions ---

@st.cache_data
def process_files(uploaded_files):
    """
    Reads, validates, and merges uploaded Excel files into a single DataFrame.
    """
    def extract_date_from_filename(filename):
        match = re.search(r'M(\d+)-(\d+)', filename)
        if match:
            month, year = map(int, match.groups())
            return datetime(2000 + year, month, 1), f"M{month}-{year}"
        return None, filename

    data_frames = []
    for file in uploaded_files:
        try:
            date_obj, date_label = extract_date_from_filename(file.name)
            if not date_obj:
                st.warning(f"⚠️ Could not extract date from filename: {file.name}")
                continue
            
            df = pd.read_excel(file, usecols=['SKU', 'Quantidade vendida'])
            df.rename(columns={'Quantidade vendida': date_label}, inplace=True)
            data_frames.append((date_obj, df))
        except (ValueError, KeyError):
            st.error(f"❌ File '{file.name}' is missing required columns (SKU, Quantidade vendida).")
        except Exception as e:
            st.error(f"❌ Error processing file '{file.name}': {e}")

    if not data_frames:
        return None, None

    # Sort files chronologically
    data_frames.sort(key=lambda x: x[0])
    
    # Extract sorted dataframes and month columns
    sorted_dfs = [df for _, df in data_frames]
    month_columns = [df.columns[1] for df in sorted_dfs]

    # Merge all dataframes efficiently
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='SKU', how='outer'), sorted_dfs)
    merged_df = merged_df.fillna(0).sort_values('SKU').reset_index(drop=True)
    merged_df[month_columns] = merged_df[month_columns].astype(int)
    
    return merged_df, month_columns

def create_long_format_df(df, selected_skus, month_columns):
    """
    Converts the wide-format DataFrame to a long format for easier plotting.
    """
    return df[df['SKU'].isin(selected_skus)].melt(
        id_vars=['SKU'], 
        value_vars=month_columns, 
        var_name='Month', 
        value_name='Sales'
    )

def create_download_file(df, month_columns):
    """
    Creates an in-memory Excel file for downloading.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Merged Sales Data')
        # Add summary sheet
        summary_df = df[month_columns].sum().reset_index()
        summary_df.columns = ['Month', 'Total Sales']
        summary_df.to_excel(writer, index=False, sheet_name='Monthly Summary')
    return output.getvalue()

# --- UI Layout Functions ---

def display_welcome_message():
    """Shows instructions when no files are uploaded."""
    st.info("👆 Please upload your monthly sales files to begin.")
    with st.expander("ℹ️ How to use this tool & file structure"):
        st.markdown("""
        ### 📖 Instructions:
        1. **Upload Files**: Drag and drop multiple `.xls` or `.xlsx` files.
        2. **File Naming**: Files must follow the format `M[month]-[year]` (e.g., `M7-25.xls`).
        3. **Required Columns**: Each file must contain `SKU` and `Quantidade vendida`.
        4. **Analyze & Download**: Use the tabs to visualize data and download the merged results.
        
        ### 📋 Expected File Structure:
        | SKU | Quantidade vendida | ... |
        |---|---|---|
        | ABC123 | 150 | ... |
        """)

def display_main_dashboard(df, month_columns):
    """Renders the entire dashboard after files are processed."""
    
    # --- Metrics ---
    st.subheader("🚀 Dashboard Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("📁 Files Processed", len(month_columns))
    col2.metric("🏷️ Unique SKUs", df['SKU'].nunique())
    col3.metric("📅 Months Covered", " → ".join(month_columns))

    st.markdown("---")

    # --- Central SKU Selector ---
    st.sidebar.header("⚙️ Controls")
    all_skus = sorted(df['SKU'].unique().tolist())
    default_skus = all_skus[:5] if len(all_skus) >= 5 else all_skus
    selected_skus = st.sidebar.multiselect("🎯 Select SKUs to Visualize", all_skus, default=default_skus)

    if not selected_skus:
        st.warning("Please select at least one SKU from the sidebar to see the charts.")
        return

    # Prepare data for charts
    long_df = create_long_format_df(df, selected_skus, month_columns)
    
    # --- Tabs for Visualizations ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Animated Progression", "📈 Static Comparison", "🔥 Heatmap", "🏆 Top SKUs Race"])

    with tab1:
        st.subheader("Animated SKU Sales Progression")
        animation_speed = st.sidebar.slider("⚡ Animation Speed (ms)", 200, 2000, 800, 200)

        fig = px.line(
            long_df, x='Month', y='Sales', color='SKU', markers=True,
            animation_frame='Month', animation_group='SKU',
            range_y=[0, long_df['Sales'].max() * 1.1],
            title='📈 SKU Sales Progression Over Time'
        )
        fig.update_layout(transition={'duration': animation_speed}, height=500)
        fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = animation_speed
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📊 Selected SKU Statistics")
        stats_df = df[df['SKU'].isin(selected_skus)].set_index('SKU')
        stats = pd.DataFrame({
            'Total Sales': stats_df[month_columns].sum(axis=1),
            'Average Sales': stats_df[month_columns].mean(axis=1).round(2),
            'Max Sales': stats_df[month_columns].max(axis=1),
            'Min Sales': stats_df[month_columns].min(axis=1),
        })
        st.dataframe(stats, use_container_width=True)

    with tab2:
        st.subheader("Full SKU Sales Comparison")
        fig = px.line(
            long_df, x='Month', y='Sales', color='SKU', markers=True, text='Sales',
            title='📊 SKU Sales Comparison - Full View'
        )
        fig.update_traces(textposition="top center")
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("SKU Sales Heatmap")
        heatmap_df = df[df['SKU'].isin(selected_skus)].set_index('SKU')[month_columns]
        fig = px.imshow(
            heatmap_df, text_auto=True, aspect="auto",
            labels=dict(x="Month", y="SKU", color="Sales"),
            title='🔥 SKU Sales Heatmap'
        )
        fig.update_layout(height=max(300, len(selected_skus) * 40))
        st.plotly_chart(fig, use_container_width=True)

    with tab4:
        st.subheader("Top SKUs Race by Month")
        top_n = st.sidebar.slider("🏅 Show Top N SKUs", 5, 20, 10, 5)
        
        race_data = pd.concat([
            df[['SKU', month]].nlargest(top_n, month).assign(Month=month).rename(columns={month: 'Sales'})
            for month in month_columns
        ])
        
        fig = px.bar(
            race_data, x='Sales', y='SKU', color='SKU', orientation='h',
            animation_frame='Month', title=f'🏆 Top {top_n} SKUs by Month'
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False, height=600)
        st.plotly_chart(fig, use_container_width=True)

    # --- Data Table & Download Section ---
    st.markdown("---")
    st.header("📋 Complete Merged Data Table")
    search_term = st.text_input("🔍 Search SKU", placeholder="Type to filter SKUs...")
    display_df = df[df['SKU'].astype(str).str.contains(search_term, case=False)] if search_term else df
    st.dataframe(display_df, use_container_width=True, height=400)

    st.header("⬇️ Download Options")
    excel_data = create_download_file(df, month_columns)
    st.download_button(
        label="📥 Download Full Data (Excel)",
        data=excel_data,
        file_name="merged_sku_sales.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# --- Main App ---
def main():
    st.title("📊 SKU Sales Data Merger & Analyzer")
    st.markdown("Upload your monthly sales files (format: `M7-25.xls`) to merge and analyze SKU progression.")

    uploaded_files = st.file_uploader(
        "📁 Drop your .xls/.xlsx files here",
        type=['xls', 'xlsx'],
        accept_multiple_files=True
    )

    if uploaded_files:
        merged_df, month_columns = process_files(uploaded_files)
        if merged_df is not None:
            display_main_dashboard(merged_df, month_columns)
    else:
        display_welcome_message()

    # --- Footer ---
    st.markdown("---")
    st.markdown("<div style='text-align: center; color: #666;'><p>Made with ❤️ using Streamlit & Plotly</p></div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()