# embedding_module.py
from sentence_transformers import SentenceTransformer
import numpy as np

_model = None

def get_embedding_model():
    global _model
    if _model is None:
        _model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    return _model

def embed_text(text: str) -> np.ndarray:
    if not text:
        return np.zeros(384, dtype=np.float32)  # 假设模型输出维度为384
    model = get_embedding_model()
    embedding = model.encode(text, normalize_embeddings=True, show_progress_bar=False)
    return embedding