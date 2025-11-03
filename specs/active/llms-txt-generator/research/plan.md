# Research Plan: `llms.txt` Specification and Best Practices

## Objective
To define the format and content strategy for `llms.txt` and `llms-full.txt` by adopting the existing `llms.txt` convention pioneered by projects like LangChain and LangGraph.

## Research Findings
Initial web searches for a formal, universal `llms.txt` specification were inconclusive. However, the user provided a crucial link to the LangGraph documentation, which revealed a well-defined convention.

- **Source**: [LangGraph `llms.txt` Overview](https://langchain-ai.github.io/langgraph/llms-txt-overview/)
- **Specification**: The format is a Markdown file that serves as a high-level index for LLMs. It contains a curated list of named links to key documentation pages, each with a concise description of the linked content.
- **`llms.txt` vs. `llms-full.txt`**:
    - `llms.txt`: A concise index file with links. The LLM must follow the links for full context.
    - `llms-full.txt`: A comprehensive file that includes all the content from the linked documents directly. This is suitable for IDEs with RAG capabilities that can index the entire file.

## Decision
The research phase is complete. We will adopt the LangGraph `llms.txt` Markdown-based index format. This approach has several advantages:
- It follows an emerging community standard.
- It is both human-readable and easily parsable.
- It provides a clear, two-tiered approach to context provisioning (`llms.txt` for a summary, `llms-full.txt` for a deep dive).

The technical design in the `prd.md` will be updated to reflect this specific format.
