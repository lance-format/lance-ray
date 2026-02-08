# Quick Reference: Memory in Lance-Ray / 快速参考：Lance-Ray中的内存

## 🔍 Important Notice / 重要提示

**This repository does NOT contain nanobot code.**  
**本仓库不包含 nanobot 代码。**

This is a data processing library (lance-ray), not an AI agent framework.  
这是一个数据处理库（lance-ray），而不是AI代理框架。

---

## 📊 What Memory Management Exists Here / 这里存在什么内存管理

### 1. Lazy Loading / 延迟加载 🔄
```python
# Dataset only loaded when first accessed
@property
def lance_dataset(self):
    if self._lance_ds is None:
        self._lance_ds = lance.dataset(...)
    return self._lance_ds
```

### 2. Fragment-Based Processing / 分片处理 📦
```
Large Dataset → Split into Fragments → Process in Parallel → Combine Results
大型数据集 → 分割成片段 → 并行处理 → 合并结果
```

### 3. Streaming / 流式处理 🌊
```python
# Process one batch at a time
for batch in scanner.to_reader():
    yield pa.Table.from_batches([batch])
```

### 4. Memory Limits / 内存限制 ⚙️
```python
max_rows_per_file = 64 * 1024 * 1024  # 64M rows
max_bytes_per_file = 90 * 1024 * 1024 * 1024  # 90GB
```

### 5. Retry Logic / 重试逻辑 🔁
```python
@retry(max_attempts=10, exponential_backoff=True)
def read_fragments(...):
    # Automatic retry on failure
```

---

## 🤖 If You're Looking for Nanobot Agent Memory / 如果你在寻找Nanobot代理内存

### Typical Nanobot Memory Components / 典型的Nanobot内存组件:

```python
class AgentMemory:
    """This is what you might find in a nanobot/agent system"""
    
    # Short-term memory: Recent conversation
    # 短期记忆：最近的对话
    conversation_history: List[Message]
    
    # Long-term memory: Vector database
    # 长期记忆：向量数据库
    vector_store: ChromaDB | Pinecone | Weaviate
    
    # Working memory: Current task state
    # 工作记忆：当前任务状态
    current_context: Dict[str, Any]
    
    def remember(self, text: str):
        """Store in long-term memory"""
        embedding = embed(text)
        self.vector_store.add(embedding)
    
    def recall(self, query: str) -> List[str]:
        """Retrieve from long-term memory"""
        return self.vector_store.search(query, k=5)
```

### Technologies Used in Nanobot Memory / Nanobot内存中使用的技术:
- **Vector Databases** / 向量数据库: ChromaDB, Pinecone, Weaviate, FAISS
- **Embeddings** / 嵌入: OpenAI, Sentence Transformers
- **Agent Frameworks** / 代理框架: LangChain, LlamaIndex, AutoGPT
- **Caching** / 缓存: Redis, Memcached

---

## 📋 Comparison Table / 对比表

| Feature | Nanobot (AI Agent) | Lance-Ray (Data Processing) |
|---------|-------------------|----------------------------|
| **Purpose** / 目的 | AI agent memory | Data processing |
| **Memory Type** / 内存类型 | Conversational | File-based |
| **Storage** / 存储 | Vector DB | Lance format |
| **Retrieval** / 检索 | Semantic search | SQL-like queries |
| **Size** / 大小 | MB to GB | GB to TB |
| **Persistence** / 持久化 | Database | File system |

---

## ✅ Summary / 总结

### This Repository (lance-ray) / 本仓库 (lance-ray):
- ✅ **Is**: A distributed data processing library
- ✅ **Has**: Advanced memory management for large datasets
- ✅ **Uses**: Lazy loading, streaming, fragmentation
- ❌ **Is NOT**: An AI agent framework
- ❌ **Does NOT have**: Nanobot-style agent memory

### To Find Nanobot / 查找Nanobot:
1. Check if nanobot is in a different repository / 检查nanobot是否在不同的仓库中
2. Look for projects using LangChain/LlamaIndex / 寻找使用LangChain/LlamaIndex的项目
3. Search for vector database implementations / 搜索向量数据库实现

---

## 📖 Full Documentation / 完整文档

For detailed technical analysis, see:
- **NANOBOT_MEMORY_ANALYSIS.md** - Situation explanation
- **docs/LANCE_RAY_MEMORY_ARCHITECTURE.md** - Complete technical details

---

## 🔗 Useful Links / 有用链接

- [Lance Format Documentation](https://lance.org/)
- [Ray Documentation](https://docs.ray.io/)
- [LangChain (for agent memory)](https://langchain.com/)
- [LlamaIndex (for agent memory)](https://llamaindex.ai/)

---

**Created** / 创建日期: 2026-02-08  
**Repository** / 仓库: lance-format/lance-ray
