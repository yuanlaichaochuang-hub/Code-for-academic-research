#功能：公众号去重

# 导入核心数据处理库pandas
# 旧版
import pandas as pd
import time
import re
from zhconv import convert  # 用于繁简体转换

def clean_mp_name(name):
    """
    严格清洗公众号名称，消除隐形差异
    :param name: 原始公众号名称
    :return: 清洗后的标准名称
    """
    if pd.isna(name):  # 处理空值
        return ""
    # 1. 转为字符串，去除首尾空格、中间多余空格、制表符、换行符
    clean_name = str(name).strip()  # 去首尾空格
    clean_name = re.sub(r"\s+", "", clean_name)  # 去除所有空格/制表符/换行
    # 2. 全半角转换（统一为半角）
    clean_name = clean_name.translate(str.maketrans(
        {'０':'0','１':'1','２':'2','３':'3','４':'4','５':'5','６':'6','７':'7','８':'8','９':'9',
         'ａ':'a','ｂ':'b','ｃ':'c','ｄ':'d','ｅ':'e','ｆ':'f','ｇ':'g','ｈ':'h','ｉ':'i','ｊ':'j',
         'ｋ':'k','ｌ':'l','ｍ':'m','ｎ':'n','ｏ':'o','ｐ':'p','ｑ':'q','ｒ':'r','ｓ':'s','ｔ':'t',
         'ｕ':'u','ｖ':'v','ｗ':'w','ｘ':'x','ｙ':'y','ｚ':'z',
         'Ａ':'A','Ｂ':'B','Ｃ':'C','Ｄ':'D','Ｅ':'E','Ｆ':'F','Ｇ':'G','Ｈ':'H','Ｉ':'I','Ｊ':'J',
         'Ｋ':'K','Ｌ':'L','Ｍ':'M','Ｎ':'N','Ｏ':'O','Ｐ':'P','Ｑ':'Q','Ｒ':'R','Ｓ':'S','Ｔ':'T',
         'Ｕ':'U','Ｖ':'V','Ｗ':'W','Ｘ':'X','Ｙ':'Y','Ｚ':'Z',
         '，':',','。':'.','？':'?','！':'!','：':':','；':';','（':'(',')':')','【':'[',']':']'}))
    # 3. 繁简体统一（转为简体）
    clean_name = convert(clean_name, 'zh-cn')
    # 4. 去除不可见字符（如零宽空格）
    clean_name = re.sub(r'[\u200b\u200c\u200d\u2060\uFEFF]', '', clean_name)
    return clean_name




# 导入核心数据处理库pandas

def extract_unique_mp_accounts2(excel_path, output_csv_path="unique_mp_accounts.csv"):
    """
    从Excel文件中按mpname列去重，重命名为Label，新增ID列，仅保留Label和jour_dis列
    :param excel_path: 你的Excel文件路径（必填，如："你的数据.xlsx"）
    :param output_csv_path: 输出CSV文件路径（默认：unique_mp_accounts.csv）
    """
    # 记录开始时间
    start_time = time.time()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 【节点处理】开始执行：读取Excel文件...")
    
    try:
        # 1. 读取Excel文件，自动识别表头
        df = pd.read_excel(excel_path)
        print(f"成功读取Excel文件，原始数据总行数：{len(df)}")
        
        # 2. 检查mpname列是否存在（核心去重列）
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 检查mpname列是否存在...")
        if "mpname" not in df.columns:
            raise ValueError("Excel文件中未找到mpname列，请检查列名是否正确")
        

        # 3. 清洗名称：新增清洗后的列，作为去重和ID的依据
        df["clean_name"] = df["mpname"].apply(clean_mp_name)
        print("完成公众号名称清洗")
        

        # 3. 按mpname列去重，保留每条公众号的第一条完整记录
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 按mpname列去重...")
        unique_mp_df = df.drop_duplicates(subset=["clean_name"], keep="first")
        print(f"清洗后去重，剩余总行数：{len(unique_mp_df)}")

        # 4. 仅保留需要的列：clean_name和jour_dis
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 筛选并重命名列...")
        required_columns = ["clean_name", "jour_dis"]
        existing_columns = [col for col in required_columns if col in unique_mp_df.columns]
        if not existing_columns:
            raise ValueError(f"Excel文件中未找到必需的列，请检查列名是否正确（需要包含 {required_columns} 中的至少一个）")
        print(f"找到的列: {existing_columns}")


        # 只保留需要的列
        unique_mp_df = unique_mp_df[existing_columns]
        
        # 5. 将mpname重命名为Label
        unique_mp_df.rename(columns={"clean_name": "Label"}, inplace=True)
        
        # 6. 新增ID列
        unique_mp_df["ID"] = unique_mp_df["Label"]
        
        # 7. 调整列的显示顺序（ID在前，Label次之，jour_dis最后，其余列跟在后面）
        preferred_order = ["ID", "Label", "jour_dis"]
        # 过滤出存在的列，并保持 preferred_order 的顺序
        ordered_columns = [col for col in preferred_order if col in unique_mp_df.columns]
        # 将剩余的、不在 preferred_order 中的列追加到后面
        remaining_columns = [col for col in unique_mp_df.columns if col not in preferred_order]
        ordered_columns.extend(remaining_columns)
        unique_mp_df = unique_mp_df[ordered_columns]

        
        # 8. 输出去重后的数据为CSV文件，避免中文乱码
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 输出去重后数据为CSV...")
        unique_mp_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
        
        # 9. 打印运行日志
        print(f"成功读取Excel文件：{excel_path}")
        print(f"原始数据总行数：{len(df)}")
        print(f"去重后公众号总数：{len(unique_mp_df)}")
        print(f"已输出数据到：{output_csv_path}")
        print(f"输出列：{list(unique_mp_df.columns)}")
        
        # 计算并输出总耗时
        total_time = time.time() - start_time
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] 执行完成，总耗时：{total_time:.2f}秒")
        
        return unique_mp_df  # 返回处理后的DataFrame，方便后续使用
    
    except FileNotFoundError:
        print(f"错误：未找到指定的Excel文件，请检查路径是否正确：{excel_path}")
    except Exception as e:
        print(f"程序异常：{str(e)}")







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
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 【边处理】开始执行：读取Excel文件...")

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


        # 3. 清洗名称：新增清洗后的列，作为去重和ID的依据
        df["clean_name"] = df["mpname"].apply(clean_mp_name)
        print("完成公众号名称清洗")

        article_mp_dict = {}
        for title, group_df in df.groupby("title"):
            unique_mps = group_df["clean_name"].drop_duplicates().tolist()
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





# -------------------------- 核心调用代码（修改路径即可使用） --------------------------
if __name__ == "__main__":
    # 请手动修改：你的Excel文件路径（绝对路径或相对路径均可）
    year=2023
    excel_path = fr"E:\文档\课件\大四上\毕业论文\代码\origin_data\{year}.xlsx"  # 示例："D:/研究数据/微信公众号数据.xlsx"
    # 可选修改：输出CSV文件路径
    output_node = fr"E:\文档\课件\大四上\毕业论文\代码\data\{year}node.csv"
    output_edge = fr"E:\文档\课件\大四上\毕业论文\代码\data\{year}edge.csv"
    
    print(f'----------------{year}年开始处理----------------')
    # 执行提取函数node
    extract_unique_mp_accounts2(excel_path=excel_path, output_csv_path=output_node)

    # 执行提取函数edge
    extract_mp_edges(excel_path=excel_path, output_csv_path=output_edge)
    print(f'----------------{year}年处理完成----------------')