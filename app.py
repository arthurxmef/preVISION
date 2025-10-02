import streamlit as st
import pandas as pd
import re
from datetime import datetime
import io
import plotly.graph_objects as go
import plotly.express as px
from functools import reduce

# --- Configuração da Página ---
st.set_page_config(page_title="Analisador de Vendas de SKU", layout="wide")

# --- Funções Auxiliares ---

@st.cache_data
def processar_arquivos(arquivos_enviados):
    """
    Lê, valida e consolida os arquivos Excel enviados em um único DataFrame.
    """
    def extrair_data_do_nome_arquivo(nome_arquivo):
        match = re.search(r'M(\d+)-(\d+)', nome_arquivo)
        if match:
            mes, ano = map(int, match.groups())
            return datetime(2000 + ano, mes, 1), f"M{mes}-{ano}"
        return None, nome_arquivo

    data_frames = []
    for arquivo in arquivos_enviados:
        try:
            date_obj, date_label = extrair_data_do_nome_arquivo(arquivo.name)
            if not date_obj:
                st.warning(f"⚠️ Não foi possível extrair a data do nome do arquivo: {arquivo.name}")
                continue
            
            df = pd.read_excel(arquivo, usecols=['SKU', 'Quantidade vendida'])
            df.rename(columns={'Quantidade vendida': date_label}, inplace=True)
            data_frames.append((date_obj, df))
        except (ValueError, KeyError):
            st.error(f"❌ O arquivo '{arquivo.name}' não possui as colunas obrigatórias (SKU, Quantidade vendida).")
        except Exception as e:
            st.error(f"❌ Erro ao processar o arquivo '{arquivo.name}': {e}")

    if not data_frames:
        return None, None

    # Ordenar arquivos cronologicamente
    data_frames.sort(key=lambda x: x[0])
    
    # Extrair dataframes e nomes das colunas de meses
    dfs_ordenados = [df for _, df in data_frames]
    colunas_meses = [df.columns[1] for df in dfs_ordenados]

    # Consolidar todos os dataframes de forma eficiente
    df_consolidado = reduce(lambda left, right: pd.merge(left, right, on='SKU', how='outer'), dfs_ordenados)
    df_consolidado = df_consolidado.fillna(0).sort_values('SKU').reset_index(drop=True)
    df_consolidado[colunas_meses] = df_consolidado[colunas_meses].astype(int)
    
    return df_consolidado, colunas_meses

def criar_df_formato_longo(df, skus_selecionados, colunas_meses):
    """
    Converte o DataFrame de formato "largo" para "longo" para facilitar a plotagem.
    """
    return df[df['SKU'].isin(skus_selecionados)].melt(
        id_vars=['SKU'], 
        value_vars=colunas_meses, 
        var_name='Mês', 
        value_name='Vendas'
    )

def criar_arquivo_download(df, colunas_meses):
    """
    Cria um arquivo Excel em memória para download.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados de Vendas Consolidados')
        # Adicionar aba de resumo
        df_resumo = df[colunas_meses].sum().reset_index()
        df_resumo.columns = ['Mês', 'Vendas Totais']
        df_resumo.to_excel(writer, index=False, sheet_name='Resumo Mensal')
    return output.getvalue()

# --- Funções de Layout da UI ---

def exibir_mensagem_boas_vindas():
    """Mostra instruções quando nenhum arquivo é enviado."""
    st.info("👆 Por favor, envie seus arquivos de vendas mensais para começar.")
    with st.expander("ℹ️ Como usar a ferramenta e estrutura dos arquivos"):
        st.markdown("""
        ### 📖 Instruções:
        1. **Envie os Arquivos**: Arraste e solte múltiplos arquivos `.xls` ou `.xlsx`.
        2. **Nomenclatura**: Os arquivos devem seguir o formato `M[mês]-[ano]` (ex: `M7-25.xls`).
        3. **Colunas Obrigatórias**: Cada arquivo deve conter as colunas `SKU` e `Quantidade vendida`.
        4. **Analise e Baixe**: Use as abas para visualizar os dados e baixar os resultados consolidados.
        
        ### 📋 Estrutura Esperada do Arquivo:
        | SKU | Quantidade vendida | ... |
        |---|---|---|
        | ABC123 | 150 | ... |
        """)

def exibir_painel_principal(df, colunas_meses):
    """Renderiza todo o painel após o processamento dos arquivos."""
    
    # --- Métricas ---
    st.subheader("🚀 Resumo do Painel")
    col1, col2, col3 = st.columns(3)
    col1.metric("📁 Arquivos Processados", len(colunas_meses))
    col2.metric("🏷️ SKUs Únicos", df['SKU'].nunique())
    col3.metric("📅 Meses Analisados", " → ".join(colunas_meses))

    st.markdown("---")

    # --- Seletor Central de SKU ---
    st.sidebar.header("⚙️ Controles")
    todos_skus = sorted(df['SKU'].unique().tolist())
    skus_padrao = todos_skus[:5] if len(todos_skus) >= 5 else todos_skus
    skus_selecionados = st.sidebar.multiselect("🎯 Selecione os SKUs para Visualizar", todos_skus, default=skus_padrao)

    if not skus_selecionados:
        st.warning("Por favor, selecione ao menos um SKU na barra lateral para ver os gráficos.")
        return

    # Preparar dados para os gráficos
    df_longo = criar_df_formato_longo(df, skus_selecionados, colunas_meses)
    
    # --- Abas para Visualizações ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Progressão Animada", "📈 Comparação Estática", "🔥 Mapa de Calor", "🏆 Corrida de Top SKUs"])

    with tab1:
        st.subheader("Progressão Animada de Vendas por SKU")
        velocidade_animacao = st.sidebar.slider("⚡ Velocidade da Animação (ms)", 200, 2000, 800, 200)

        fig = px.line(
            df_longo, x='Mês', y='Vendas', color='SKU', markers=True,
            animation_frame='Mês', animation_group='SKU',
            range_y=[0, df_longo['Vendas'].max() * 1.1],
            title='📈 Progressão de Vendas de SKU ao Longo do Tempo',
            labels={'Vendas': 'Quantidade Vendida', 'Mês': 'Mês'}
        )
        fig.update_layout(transition={'duration': velocidade_animacao}, height=500)
        fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = velocidade_animacao
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📊 Estatísticas dos SKUs Selecionados")
        stats_df = df[df['SKU'].isin(skus_selecionados)].set_index('SKU')
        stats = pd.DataFrame({
            'Vendas Totais': stats_df[colunas_meses].sum(axis=1),
            'Média de Vendas': stats_df[colunas_meses].mean(axis=1).round(2),
            'Venda Máxima': stats_df[colunas_meses].max(axis=1),
            'Venda Mínima': stats_df[colunas_meses].min(axis=1),
        })
        st.dataframe(stats, use_container_width=True)

    with tab2:
        st.subheader("Comparação Completa de Vendas por SKU")
        fig = px.line(
            df_longo, x='Mês', y='Vendas', color='SKU', markers=True, text='Vendas',
            title='📊 Comparação de Vendas de SKU - Visão Completa',
            labels={'Vendas': 'Quantidade Vendida', 'Mês': 'Mês'}
        )
        fig.update_traces(textposition="top center")
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Mapa de Calor de Vendas por SKU")
        df_heatmap = df[df['SKU'].isin(skus_selecionados)].set_index('SKU')[colunas_meses]
        fig = px.imshow(
            df_heatmap, text_auto=True, aspect="auto",
            labels=dict(x="Mês", y="SKU", color="Vendas"),
            title='🔥 Mapa de Calor de Vendas por SKU'
        )
        fig.update_layout(height=max(300, len(skus_selecionados) * 40))
        st.plotly_chart(fig, use_container_width=True)

    with tab4:
        st.subheader("Corrida dos Top SKUs por Mês")
        top_n = st.sidebar.slider("🏅 Exibir Top N SKUs", 5, 20, 10, 5)
        
        dados_corrida = pd.concat([
            df[['SKU', mes]].nlargest(top_n, mes).assign(Mês=mes).rename(columns={mes: 'Vendas'})
            for mes in colunas_meses
        ])
        
        fig = px.bar(
            dados_corrida, x='Vendas', y='SKU', color='SKU', orientation='h',
            animation_frame='Mês', title=f'🏆 Top {top_n} SKUs por Mês',
            labels={'Vendas': 'Quantidade Vendida', 'Mês': 'Mês'}
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False, height=600)
        st.plotly_chart(fig, use_container_width=True)

    # --- Tabela de Dados e Seção de Download ---
    st.markdown("---")
    st.header("📋 Tabela de Dados Consolidados")
    termo_busca = st.text_input("🔍 Buscar SKU", placeholder="Digite para filtrar SKUs...")
    df_exibicao = df[df['SKU'].astype(str).str.contains(termo_busca, case=False)] if termo_busca else df
    st.dataframe(df_exibicao, use_container_width=True, height=400)

    st.header("⬇️ Opções de Download")
    dados_excel = criar_arquivo_download(df, colunas_meses)
    st.download_button(
        label="📥 Baixar Dados Completos (Excel)",
        data=dados_excel,
        file_name="vendas_sku_consolidadas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# --- App Principal ---
def main():
    st.title("📊 Consolidador e Analisador de Vendas por SKU")
    st.markdown("Envie seus arquivos de vendas mensais (formato: `M7-25.xls`) para consolidar e analisar a progressão.")

    arquivos_enviados = st.file_uploader(
        "📁 Arraste e solte seus arquivos .xls/.xlsx aqui",
        type=['xls', 'xlsx'],
        accept_multiple_files=True
    )

    if arquivos_enviados:
        df_consolidado, colunas_meses = processar_arquivos(arquivos_enviados)
        if df_consolidado is not None:
            exibir_painel_principal(df_consolidado, colunas_meses)
    else:
        exibir_mensagem_boas_vindas()

    # --- Rodapé ---
    st.markdown("---")
    st.markdown("<div style='text-align: center; color: #666;'><p>Feito com ❤️ usando Streamlit & Plotly</p></div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
