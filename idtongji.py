import pandas as pd
import numpy as np

# ==================== 核心配置（只需修改这3个路径！）====================
# 你的CSV/Excel文件路径（支持.csv或.xlsx）
INPUT_FILE = fr"E:\文档\课件\大四\毕业论文\代码\comnode_new.xlsx"  # 替换为你的文件路径，如 "comnode_2.csv"
# 输出Excel文件路径
OUTPUT_EXCEL = "社区身份统计结果new.xlsx"
# 若输入是Excel，指定工作表名称（默认Sheet1）
SHEET_NAME = "Sheet1"
# =====================================================================

# ==================== 固定列名（根据你提供的列名直接写入）====================
IDENTITY_COL = "主体类型"    # 身份列
COMMUNITY_COL = "modularity_class"  # 社区列（对应Modularity Class）
# =====================================================================

def calculate_identity_concentration(df, identity_col, community_col):
    """统计1：单身份的聚类集中度"""
    # 1. 按「身份+社区」分组统计节点数
    identity_community_count = df.groupby([identity_col, community_col]).size().reset_index(name="节点数")
    # 2. 计算每个身份的总节点数
    identity_total = df.groupby(identity_col).size().reset_index(name="身份总节点数")
    # 3. 合并数据
    identity_stats = pd.merge(identity_community_count, identity_total, on=identity_col)
    # 4. 对每个身份取Top2社区
    top2_list = []
    for identity in identity_stats[identity_col].unique():
        sub = identity_stats[identity_stats[identity_col] == identity].sort_values("节点数", ascending=False).head(2)
        top2_list.append(sub)
    top2_df = pd.concat(top2_list, ignore_index=True)
    # 5. 计算集中度
    concentration_result = []
    for identity in top2_df[identity_col].unique():
        identity_top2 = top2_df[top2_df[identity_col] == identity]
        total_top2 = identity_top2["节点数"].sum()
        total_identity = identity_total[identity_total[identity_col] == identity]["身份总节点数"].iloc[0]
        concentration = (total_top2 / total_identity) * 100
        # 分类
        if concentration > 60:
            level = "极强聚类特征（＞60%）"
        elif 30 <= concentration <= 60:
            level = "中等聚类特征（30%-60%）"
        else:
            level = "分散分布（＜30%）"
        # Top2社区描述
        top2_desc = ", ".join([f"社区{int(c)}（{n}个节点）" for c, n in zip(identity_top2[community_col], identity_top2["节点数"])])
        concentration_result.append({
            "身份": identity,
            "身份总节点数": total_identity,
            "Top2社区及节点数": top2_desc,
            "聚类集中度（%）": round(concentration, 2),
            "聚类特征等级": level
        })
    return pd.DataFrame(concentration_result)

def calculate_community_identity_composition(df, identity_col, community_col):
    """统计2：单个社区的身份构成"""
    # 1. 按「社区+身份」分组统计
    community_identity_count = df.groupby([community_col, identity_col]).size().reset_index(name="身份节点数")
    # 2. 计算社区总节点数
    community_total = df.groupby(community_col).size().reset_index(name="社区总节点数")
    # 3. 合并并计算占比
    community_stats = pd.merge(community_identity_count, community_total, on=community_col)
    community_stats["身份占比（%）"] = round((community_stats["身份节点数"] / community_stats["社区总节点数"]) * 100, 2)
    # 4. 整理每个社区的身份构成
    composition_list = []
    core_summary = []
    for community in community_stats[community_col].unique():
        sub = community_stats[community_stats[community_col] == community].sort_values("身份占比（%）", ascending=False)
        # 身份构成描述
        comp_desc = ", ".join([f"{i}（{r}%）" for i, r in zip(sub[identity_col], sub["身份占比（%）"])])
        # 核心身份
        core_identity = sub.iloc[0][identity_col]
        core_ratio = sub.iloc[0]["身份占比（%）"]
        core_attr = f"社区{int(community)}：{core_ratio}% 为{core_identity}，属于{core_identity}核心聚类"
        composition_list.append({
            "社区编号": int(community),
            "社区总节点数": sub["社区总节点数"].iloc[0],
            "身份构成（按占比降序）": comp_desc,
            "核心身份": core_identity,
            "核心身份占比（%）": core_ratio,
            "社区核心属性": core_attr
        })
        core_summary.append(core_attr)
    return pd.DataFrame(composition_list).sort_values("社区编号"), core_summary

def export_to_excel(concentration_df, composition_df, output_path):
    """导出到Excel，分两个工作表"""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        concentration_df.to_excel(writer, sheet_name="单身份聚类集中度", index=False)
        composition_df.to_excel(writer, sheet_name="单个社区身份构成", index=False)
    print(f"\n✅ 结果已导出到：{output_path}")

# ==================== 一键执行主程序 ====================
if __name__ == "__main__":
    print("="*50)
    print("📊 社区-身份统计程序（一键运行版）")
    print("="*50)
    
    # 1. 读取数据
    try:
        if INPUT_FILE.endswith(".csv"):
            df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
        elif INPUT_FILE.endswith(".xlsx"):
            df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
        else:
            raise ValueError("仅支持.csv或.xlsx格式文件！")
        print(f"✅ 成功读取文件：{INPUT_FILE}，总行数：{len(df)}")
    except Exception as e:
        print(f"❌ 读取文件失败：{str(e)}")
        exit()
    
    # 2. 数据清洗（去除关键列缺失值）
    df_clean = df.dropna(subset=[IDENTITY_COL, COMMUNITY_COL]).copy()
    print(f"🔍 去除缺失值后行数：{len(df_clean)}（原始：{len(df)}）")
    
    # 3. 统计1：单身份聚类集中度
    print("\n🔍 正在统计「单身份聚类集中度」...")
    concentration_result = calculate_identity_concentration(df_clean, IDENTITY_COL, COMMUNITY_COL)
    print("✅ 单身份聚类集中度统计完成（前5条预览）：")
    print(concentration_result.head().to_string(index=False))
    
    # 4. 统计2：单个社区身份构成
    print("\n🔍 正在统计「单个社区身份构成」...")
    composition_result, core_summary = calculate_community_identity_composition(df_clean, IDENTITY_COL, COMMUNITY_COL)
    print("✅ 社区身份构成统计完成（前5条预览）：")
    print(composition_result.head().to_string(index=False))
    
    # 5. 导出结果
    print("\n📤 正在导出Excel结果...")
    export_to_excel(concentration_result, composition_result, OUTPUT_EXCEL)
    
    # 6. 打印社区核心属性摘要
    print("\n📝 社区核心属性摘要：")
    for s in core_summary[:10]:  # 先打印前10条，避免刷屏
        print(f"- {s}")
    if len(core_summary) > 10:
        print(f"... 共{len(core_summary)}个社区，完整结果请查看Excel")
    
    print("\n🎉 所有统计完成！结果已保存到：", OUTPUT_EXCEL)