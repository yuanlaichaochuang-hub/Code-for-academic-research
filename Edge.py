#功能：统计所有公众号间两两共引数量

# 导入核心库
import pandas as pd
import itertools
import time

def extract_mp_edges(excel_path, output_csv_path="mp_citation_edges.csv"):
    """
    提取公众号共引文章的边信息（权重=共同引用文章数量），输出Gephi兼容的CSV格式
    :param excel_path: 你的Excel数据文件路径
    :param output_csv_path: 输出边表CSV路径
    """
    # 记录开始时间
    start_time = time.time()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 开始执行：读取Excel文件...")

    try:
        # 1. 读取Excel文件，兼容.xlsx/.xls格式
        df = pd.read_excel(excel_path)
        print(f"成功读取Excel文件，原始数据总行数：{len(df)}")

        # 2. 检查关键列是否存在（标题=文章标识，mpname=公众号标识）
        required_cols = ["title", "mpname"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Excel文件中缺少关键列：{col}，请检查列名是否正确")

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据预处理：按文章标题分组...")

        # 3. 数据预处理：按文章标题分组，提取每篇文章对应的唯一公众号列表（去重，避免自环边）
        article_mp_dict = {}
        for title, group_df in df.groupby("title"):
            unique_mps = group_df["mpname"].drop_duplicates().tolist()
            if len(unique_mps) >= 2:
                article_mp_dict[title] = unique_mps

        print(f"有效文章（至少2个公众号引用）数量：{len(article_mp_dict)}")

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 生成公众号两两组合并统计权重...")

        # 4. 生成所有公众号两两组合，并统计共引文章数（权重）
        edge_count_dict = {}
        for title, mp_list in article_mp_dict.items():
            mp_pairs = itertools.combinations(mp_list, 2)
            for mp1, mp2 in mp_pairs:
                pair_key = tuple(sorted((mp1, mp2)))
                if pair_key in edge_count_dict:
                    edge_count_dict[pair_key] += 1
                else:
                    edge_count_dict[pair_key] = 1

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 转换为边表DataFrame...")

        # 5. 转换为边表DataFrame（适配Gephi导入格式）
        edge_data = []
        for (mp1, mp2), weight in edge_count_dict.items():
            edge_data.append({
                "Source": mp1,
                "Target": mp2,
                "Weight": weight,
                "Type": "Undirected"
            })

        edge_df = pd.DataFrame(edge_data)
        edge_df = edge_df.sort_values(by="Weight", ascending=False).reset_index(drop=True)

        # 6. 输出CSV文件（utf-8-sig兼容Excel打开，无中文乱码）
        edge_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

        # 打印运行日志
        print(f"成功生成边表，总边数：{len(edge_df)}")
        print(f"边表已输出至：{output_csv_path}")
        print("\n前5条边信息预览：")
        print(edge_df.head())

        # 计算并输出总耗时
        total_time = time.time() - start_time
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] 执行完成，总耗时：{total_time:.2f}秒")

        return edge_df

    except FileNotFoundError:
        print(f"错误：未找到指定的Excel文件，请检查路径：{excel_path}")
    except Exception as e:
        print(f"程序异常：{str(e)}")

# -------------------------- 调用代码（修改路径即可使用） --------------------------
if __name__ == "__main__":
    # 请修改为你的Excel文件路径（绝对路径/相对路径均可）
    excel_path = r"E:\文档\课件\大四上\毕业论文\代码\origin_data\2021.xlsx"
    # 输出边表CSV路径（可自定义）
    output_path = r"E:\文档\课件\大四上\毕业论文\代码\data\2021edge.csv"

    # 执行函数
    extract_mp_edges(excel_path=excel_path, output_csv_path=output_path)