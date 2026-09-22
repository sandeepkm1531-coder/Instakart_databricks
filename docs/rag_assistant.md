# Instacart RAG assistant

The project owner reports building a RAG assistant after completing the Gold
layer and dashboard. The supplied document, `building RAG on top of Instackart.docx`,
describes an Instacart Dashboard Assistant in Google Colab. These notes record
that approach; the executable assistant notebook is not yet in the repository.

## Documented design

The assistant uses an analytics knowledge base containing Gold column definitions,
KPI formulas, chart descriptions, insights, recommendations, and dataset
limitations. The described stack consists of:

- `sentence-transformers` for text and question embeddings.
- FAISS for retrieving relevant knowledge-base content by vector similarity.
- Gemini through `google-genai` for answers grounded in the retrieved material.
- Google Colab as the notebook environment.

The retrieval flow embeds a question, searches FAISS, includes retrieved context
in the prompt, and asks Gemini to generate an answer. The dashboard remains in
Looker Studio; the assistant explains the information behind it.

## Example questions

- What does reorder rate mean?
- Why can't the dashboard show monthly trends?
- What does order sequence number represent?
- Explain the dashboard to a manager.

The document also describes uploading `03_Gold_Layer.csv`. It presents numerical
CSV querying as a later extension, so reliable computed answers such as the
department with the highest reorder rate are not claimed as verified features.
Numerical rankings should come from an explicit query or calculation over the
data, with the result supplied to the answer generator.

## Credentials and reproducibility

The documented setup reads `GEMINI_API_KEY` from Colab Secrets. Keep API keys out
of source control. This repository does not currently include the final Colab
notebook, knowledge-base content, pinned dependencies, model identifiers,
retrieval settings, or evaluation results. Those artifacts are needed to
reproduce and evaluate the exact assistant implementation.
