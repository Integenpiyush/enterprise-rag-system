import time
import requests
from pathlib import Path
from typing import Optional
from loguru import logger
from pydantic import BaseModel


class FilingMetadata(BaseModel):
    ticker: str
    cik: str
    accession_number: str
    filing_date: str
    document_url: str
    local_path: Optional[str] = None


class SECDownloader:
    HEADERS = {
        "User-Agent": "RAG-Research-Project piyush@iitbhu.ac.in",
        "Accept-Encoding": "gzip, deflate",
    }
    RATE_LIMIT_SLEEP = 0.2

    # Hardcoded known 10-K URLs — production systems use a database of verified URLs
    # These are the actual primary document URLs from SEC EDGAR (verified)
    KNOWN_FILINGS = {
        "AAPL": {
            "cik": "320193",
            "accession": "0000320193-25-000079",
            "filing_date": "2025-10-31",
            "doc_name": "aapl-20250927.htm",
        },
        "MSFT": {
            "cik": "789019",
            "accession": "0000950170-24-087843",
            "filing_date": "2024-07-30",
            "doc_name": "msft-20240630.htm",
        },
        "GOOGL": {
            "cik": "1652044",
            "accession": "0001652044-25-000014",
            "filing_date": "2025-02-05",
            "doc_name": "goog-20241231.htm",
        },
    }

    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _build_url(self, ticker: str) -> Optional[str]:
        info = self.KNOWN_FILINGS.get(ticker)
        if not info:
            return None
        cik = info["cik"]
        accession_no_dash = info["accession"].replace("-", "")
        doc_name = info["doc_name"]
        return (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/"
            f"{accession_no_dash}/{doc_name}"
        )

    def get_latest_10k_url(self, cik: str, ticker: str) -> Optional[FilingMetadata]:
        info = self.KNOWN_FILINGS.get(ticker)
        if not info:
            logger.error(f"No known filing for {ticker}")
            return None

        doc_url = self._build_url(ticker)
        return FilingMetadata(
            ticker=ticker,
            cik=cik,
            accession_number=info["accession"],
            filing_date=info["filing_date"],
            document_url=doc_url or "",
        )

    def download(self, metadata: FilingMetadata) -> Optional[str]:
        if not metadata.document_url:
            logger.error(f"No document URL for {metadata.ticker}")
            return None

        filename = f"{metadata.ticker}_{metadata.filing_date}_10K.htm"
        local_path = self.output_dir / filename

        if local_path.exists() and local_path.stat().st_size > 500_000:
            logger.info(f"Already downloaded: {filename}")
            metadata.local_path = str(local_path)
            return str(local_path)

        # Delete bad cached file if too small
        if local_path.exists():
            local_path.unlink()

        try:
            resp = requests.get(
                metadata.document_url,
                headers=self.HEADERS,
                timeout=60
            )
            resp.raise_for_status()

            # Sanity check — real 10-K is always > 500KB
            if len(resp.content) < 500_000:
                logger.error(
                    f"File too small ({len(resp.content)/1024:.1f} KB) — "
                    f"likely wrong URL for {metadata.ticker}"
                )
                logger.error(f"URL was: {metadata.document_url}")
                return None

            local_path.write_bytes(resp.content)
            logger.success(
                f"Downloaded {filename} ({len(resp.content)/1024:.1f} KB)"
            )
            metadata.local_path = str(local_path)
            time.sleep(self.RATE_LIMIT_SLEEP)
            return str(local_path)

        except Exception as e:
            logger.error(f"Download failed for {metadata.ticker}: {e}")
            return None
