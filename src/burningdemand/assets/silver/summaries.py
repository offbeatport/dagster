# burningdemand_dagster/assets/summaries.py
import pandas as pd
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource


def summarize_text(text: str, max_sentences: int = 3) -> str:
    """Extractive summarization using LexRank algorithm from sumy library."""
    if not text or len(text.strip()) < 50:
        return text[:500] if text else ""

    try:
        # Parse the text
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        
        # Use LexRank summarizer (graph-based algorithm using cosine similarity)
        summarizer = LexRankSummarizer()
        
        # Generate summary
        summary_sentences = summarizer(parser.document, max_sentences)
        
        # Join sentences
        summary = " ".join(str(sentence) for sentence in summary_sentences)
        
        # Limit to 1000 chars to avoid overly long summaries
        return summary[:1000] if summary else text[:500]
    except Exception:
        # Fallback to first 500 chars if summarization fails
        return text[:500]


@asset(
    partitions_def=daily_partitions,
    deps=[AssetKey(["silver", "clusters"])],
)
def summaries(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key

    # Get clusters that don't have summaries yet
    clusters = db.query_df(
        """
        SELECT sc.cluster_id, sc.cluster_size
        FROM silver.clusters sc
        WHERE sc.cluster_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM silver.cluster_summaries cs
              WHERE cs.cluster_date = sc.cluster_date
                AND cs.cluster_id = sc.cluster_id
          )
        """,
        [date],
    )

    if len(clusters) == 0:
        context.log.info(f"No new clusters to summarize for {date}")
        return MaterializeResult(metadata={"summarized": 0})

    cluster_ids = clusters["cluster_id"].astype(int).tolist()

    # Get titles and bodies for each cluster
    items_df = db.query_df(
        f"""
        SELECT cm.cluster_id, b.title, b.body
        FROM silver.cluster_members cm
        JOIN silver.embeddings s ON cm.url_hash = s.url_hash
        JOIN bronze.raw_items b ON s.url_hash = b.url_hash
        WHERE cm.cluster_date = ?
          AND cm.cluster_id IN ({",".join(["?"] * len(cluster_ids))})
        """,
        [date, *cluster_ids],
    )

    summaries_data = []
    for cid in cluster_ids:
        cluster_items = items_df[items_df["cluster_id"] == cid].head(10)
        bodies = cluster_items["body"].fillna("").astype(str).tolist()
        
        # Combine bodies and summarize
        combined_body = " ".join([b for b in bodies if b and len(b.strip()) > 0])
        summary = summarize_text(combined_body, max_sentences=3)
        
        summaries_data.append({
            "cluster_date": date,
            "cluster_id": int(cid),
            "summary": summary,
        })

    if summaries_data:
        summaries_df = pd.DataFrame(summaries_data)
        
        # Convert string columns to object dtype for DuckDB compatibility
        summaries_df["cluster_date"] = summaries_df["cluster_date"].astype("object")
        summaries_df["summary"] = summaries_df["summary"].astype("object")

        db.upsert_df(
            "silver",
            "cluster_summaries",
            summaries_df,
            ["cluster_date", "cluster_id", "summary"],
        )

    return MaterializeResult(
        metadata={"summarized": int(len(summaries_data))}
    )
