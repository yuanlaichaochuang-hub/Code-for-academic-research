import pandas as pd
import re

# ====================== 配置区域 ======================
NODE_PATH = r"comnode_new.csv"
EDGE_PATH = r"com_edge.csv"
OUTPUT_PATH = r"E:\文档\课件\大四\毕业论文\代码\身份互动.xlsx"

COL_MAPPING = {
    'node_id': 'Id',
    'node_type': '主体类型',
    'edge_source': 'Source',
    'edge_target': 'Target',
    'edge_weight': 'Weight'
}
# ==============================================================================

def clean_sheet_name(name):
    name = str(name)
    name = re.sub(r'[\\/*\[\]:?]', '_', name)
    return name[:31]

def main():
    print("🚀 开始处理无向网络数据...")

    # ---------------------- 1. 读取数据与基础检查 ----------------------
    try:
        df_nodes = pd.read_csv(NODE_PATH)
        df_edges = pd.read_csv(EDGE_PATH)
    except FileNotFoundError as e:
        print(f"❌ 错误：找不到文件。\n{e}")
        return

    required_node_cols = [COL_MAPPING['node_id'], COL_MAPPING['node_type']]
    required_edge_cols = [COL_MAPPING['edge_source'], COL_MAPPING['edge_target']]
    
    if not set(required_node_cols).issubset(df_nodes.columns):
        print(f"❌ 错误：节点表列名不对。当前：{df_nodes.columns.tolist()}")
        return
    if not set(required_edge_cols).issubset(df_edges.columns):
        print(f"❌ 错误：边表列名不对。当前：{df_edges.columns.tolist()}")
        return

    if COL_MAPPING['edge_weight'] not in df_edges.columns:
        df_edges[COL_MAPPING['edge_weight']] = 1
        print("⚠️  提示：边表未找到权重列，已自动生成。")

    # ---------------------- 2. 匹配身份并构建无向身份对 ----------------------
    print("🔗 正在匹配节点身份...")
    id_type_map = df_nodes.set_index(COL_MAPPING['node_id'])[COL_MAPPING['node_type']].to_dict()

    df_edges['身份A'] = df_edges[COL_MAPPING['edge_source']].map(id_type_map)
    df_edges['身份B'] = df_edges[COL_MAPPING['edge_target']].map(id_type_map)

    initial_count = len(df_edges)
    df_edges = df_edges.dropna(subset=['身份A', '身份B'])
    if (initial_count - len(df_edges)) > 0:
        print(f"⚠️  提示：已删除 {initial_count - len(df_edges)} 条无法匹配的边。")

    # 生成无向身份对
    df_edges['无向身份对'] = df_edges.apply(
        lambda row: tuple(sorted([row['身份A'], row['身份B']])),
        axis=1
    )

    # ---------------------- 3. 统计1：整体同身份 vs 跨身份互动 ----------------------
    print("📊 正在生成整体统计...")
    df_edges['互动类型'] = df_edges.apply(
        lambda row: '同身份互动' if row['身份A'] == row['身份B'] else '跨身份互动',
        axis=1
    )

    stats_overall = df_edges.groupby('互动类型').agg(
        关联边数=(COL_MAPPING['edge_source'], 'count'),
        总互动权重=(COL_MAPPING['edge_weight'], 'sum')
    ).reset_index()

    stats_overall['边数占比'] = stats_overall['关联边数'] / stats_overall['关联边数'].sum()
    stats_overall['权重占比'] = stats_overall['总互动权重'] / stats_overall['总互动权重'].sum()

    # ---------------------- 4. 统计2：全网络身份互动矩阵 ----------------------
    df_expanded = pd.concat([
        df_edges[['身份A', '身份B', COL_MAPPING['edge_weight']]].rename(columns={'身份A': '行身份', '身份B': '列身份'}),
        df_edges[['身份B', '身份A', COL_MAPPING['edge_weight']]].rename(columns={'身份B': '行身份', '身份A': '列身份'})
    ])

    matrix_full = df_expanded.pivot_table(
        index='行身份',
        columns='列身份',
        values=COL_MAPPING['edge_weight'],
        aggfunc='sum',
        fill_value=0
    )

    # ---------------------- 5. 统计3：逐身份详细互动统计 ----------------------
    print("🔍 正在生成逐身份详细统计表...")
    all_types = sorted(df_nodes[COL_MAPPING['node_type']].unique().tolist())
    writer = pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl')

    for current_type in all_types:
        mask = (df_edges['身份A'] == current_type) | (df_edges['身份B'] == current_type)
        df_subset = df_edges[mask].copy()

        df_subset['互动对象'] = df_subset.apply(
            lambda row: row['身份B'] if row['身份A'] == current_type else row['身份A'],
            axis=1
        )

        detail_stats = df_subset.groupby('互动对象').agg(
            关联边数=(COL_MAPPING['edge_source'], 'count'),
            总互动权重=(COL_MAPPING['edge_weight'], 'sum')
        ).reindex(all_types, fill_value=0).reset_index()

        total_edges = detail_stats['关联边数'].sum()
        total_weight = detail_stats['总互动权重'].sum()
        
        detail_stats['边数占比'] = detail_stats['关联边数'] / max(total_edges, 1)
        detail_stats['权重占比'] = detail_stats['总互动权重'] / max(total_weight, 1)

        detail_stats = detail_stats[['互动对象', '关联边数', '边数占比', '总互动权重', '权重占比']]

        sum_row = pd.DataFrame({
            '互动对象': ['【该身份合计】'],
            '关联边数': [total_edges],
            '边数占比': [1.0],
            '总互动权重': [total_weight],
            '权重占比': [1.0]
        })
        detail_stats = pd.concat([detail_stats, sum_row], ignore_index=True)

        sheet_name = clean_sheet_name(f"详情_{current_type}")
        detail_stats.to_excel(writer, sheet_name=sheet_name, index=False)

    # ---------------------- 6. 统计4：两两身份配对总表（已修复） ----------------------
    print("📋 正在生成两两配对表...")
    
    # 【修复逻辑】直接按无向身份对分组聚合，然后拆分元组列
    stats_pairwise = df_edges.groupby('无向身份对').agg(
        关联边数=(COL_MAPPING['edge_source'], 'count'),
        总互动权重=(COL_MAPPING['edge_weight'], 'sum')
    ).reset_index()

    # 将元组列(身份1, 身份2)拆分成两列
    stats_pairwise[['身份1', '身份2']] = pd.DataFrame(stats_pairwise['无向身份对'].tolist(), index=stats_pairwise.index)

    # 调整列顺序并删除辅助列
    stats_pairwise = stats_pairwise[['身份1', '身份2', '关联边数', '总互动权重']]
    
    # 按权重降序排列
    stats_pairwise = stats_pairwise.sort_values(by='总互动权重', ascending=False)

    # ---------------------- 7. 导出所有结果 ----------------------
    print("💾 正在导出Excel文件...")
    
    stats_overall.to_excel(writer, sheet_name='1.整体统计', index=False)
    matrix_full.to_excel(writer, sheet_name='2.全网络互动矩阵')
    stats_pairwise.to_excel(writer, sheet_name='3.两两身份配对表', index=False)

    writer.close()
    print(f"✅ 无向网络统计完成！\n结果已保存至：{OUTPUT_PATH}")

if __name__ == "__main__":
    main()