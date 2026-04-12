import pandas as pd
import jieba
import re
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# 1. 加载停用词（内置基础停用词，可替换为自定义停用词表）
STOPWORDS = set([
    '的', '了', '在', '是', '我', '你', '他', '她', '它', '们', '和', '与', '及', '或',
    '之', '于', '也', '都', '还', '有', '为', '因', '以', '而', '则', '即', '若', '如',
    '个', '本', '册', '期', '刊', '报', '志', '编辑部', '研究', '中心', '学院', 
    '动态', '通讯', '论坛', '在线', '微刊'  # 可根据你的Label特征补充
])

def preprocess_data(csv_path):
    """
    通用数据预处理：读取CSV、过滤class0、按社区分组、Label分词
    :param csv_path: 你的CSV文件路径
    :return: 字典（key=社区编号，value=该社区所有Label分词后的词列表）
    """
    # 读取数据
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    print(f"原始数据总行数：{len(df)}")
    
    # 过滤class0，并重命名列（兼容大小写/拼写）
    df.rename(columns={"modularity_class": "class"}, inplace=True)
    df = df[df["class"] != 0].reset_index(drop=True)
    print(f"过滤class0后行数：{len(df)}")
    
    # 按社区分组，Label去重（避免同一公众号重复计算）
    df_grouped = df.groupby("class")["Label"].unique().reset_index()
    print(f"有效社区数量：{len(df_grouped)}")
    
    # 定义文本清洗+分词函数
    def clean_and_cut(text):
        # 清洗：去除标点、数字、空格，仅保留中文
        text = re.sub(r"[^\u4e00-\u9fa5]", "", str(text).strip())
        # 分词
        words = jieba.lcut(text)
        # 过滤停用词和单字
        words = [w for w in words if w not in STOPWORDS and len(w) > 1]
        return words
    
    # 按社区处理Label
    community_words = {}
    for _, row in df_grouped.iterrows():
        class_id = row["class"]
        labels = row["Label"]
        # 对每个Label分词，合并为该社区的词列表
        all_words = []
        for label in labels:
            all_words.extend(clean_and_cut(label))
        community_words[class_id] = all_words
        print(f"社区{class_id}：分词后总词数={len(all_words)}")
    
    return community_words, df_grouped

#----词频----
def community_word_frequency(community_words, top_n=20, output_path="社区词频结果.csv"):
    """
    按社区统计词频，输出TOPN关键词
    :param community_words: 预处理后的社区-词列表字典
    :param top_n: 每个社区输出TOPN关键词
    :param output_path: 结果输出路径
    """
    # 统计每个社区的词频
    result = []
    for class_id, words in community_words.items():
        if not words:
            result.append({"class": class_id, "top_keywords": "无有效关键词", "词频": ""})
            continue
        # 统计词频
        word_count = Counter(words)
        # 取TOPN
        top_words = word_count.most_common(top_n)
        # 格式化结果
        keywords = [w[0] for w in top_words]
        frequencies = [w[1] for w in top_words]
        result.append({
            "class": class_id,
            "top_keywords": ",".join(keywords),
            "词频": ",".join(map(str, frequencies))
        })
    
    # 保存结果
    df_result = pd.DataFrame(result)
    df_result.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n词频结果已保存至：{output_path}")
    
    # 打印示例
    print("\n=== 社区词频TOP5示例 ===")
    for class_id, words in community_words.items():
        if words:
            top5 = Counter(words).most_common(5)
            print(f"社区{class_id} TOP5：{[w[0] for w in top5]}")
            break  # 仅打印第一个社区示例
    
    return df_result

# ========== 词频方案调用 ==========
if __name__ == "__main__":
    # 替换为你的CSV路径
    CSV_PATH = fr"E:\文档\课件\大四上\毕业论文\代码\data\导出节点表.csv"
    # 预处理数据
    community_words, df_grouped = preprocess_data(CSV_PATH)
    # 统计词频
    freq_result = community_word_frequency(community_words, top_n=12)
