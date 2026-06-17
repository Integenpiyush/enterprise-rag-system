"""
SEC EDGAR 10-K Downloader
-------------------------
Why EDGAR directly (not Kaggle datasets)?
- Real-world pipeline: companies expect you to pull live data from primary sources
- Demonstrates API integration skills, not just notebook gymnastics
- Data freshness — always the latest filings

Interview angle: "How would you handle rate limiting on EDGAR?"
Answer is in the retry logic below.
"""
import time
import requests
from pathlib import Path
from typing import Optional
from loguru import logger
from pydantic import BaseModel


class FilingMetadata(BaseModel):
    """Pydantic schema for filing metadata — enforces type safety throughout pipeline."""
    ticker: str
    cik: str
    accession_number: str
    filing_date: str
    document_url: str
    local_path: Optional[str] = None


class SECDownloader:
    """
    Downloads 10-K filings from SEC EDGAR.

    Design decisions:
    - Looks up the latest 10-K dynamically via EDGAR's submissions API,
      instead of hardcoding accession numbers (which go stale every filing
      cycle — see this repo's history for a real example of that bug).
    - 10 req/sec EDGAR rate limit → we sleep between requests as a safety margin.
    - User-Agent header is REQUIRED by SEC — missing/generic one risks an IP ban.
    - We download the actual primary HTM document, not the filing index page.
    """

    SUBMISSIONS_BASE_URL = "https://data.sec.gov"
    ARCHIVES_BASE_URL = "https://www.sec.gov"
    HEADERS = {
        "User-Agent": "RAG-Research-Project piyush@iitbhu.ac.in",
        "Accept-Encoding": "gzip, deflate",
    }
    RATE_LIMIT_SLEEP = 0.2  # seconds between requests

    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_latest_10k_url(self, cik: str, ticker: str) -> Optional[FilingMetadata]:
        """
        Fetches the most recent 10-K filing metadata for a given CIK.
        Failure mode: CIK not found or no 10-K in recent filings → returns None.
        """
        cik_padded = cik.zfill(10)
        url = f"{self.SUBMISSIONS_BASE_URL}/submissions/CIK{cik_padded}.json"

        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            time.sleep(self.RATE_LIMIT_SLEEP)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch submissions for {ticker}: {e}")
            return None

        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])

        for i, form in enumerate(forms):
            if form == "10-K":
                accession_raw = filings["accessionNumber"][i]
                accession_no_dash = accession_raw.replace("-", "")
                filing_date = filings["filingDate"][i]

                doc_url = self._get_primary_document_url(
                    cik_padded, accession_raw, accession_no_dash, ticker
                )

                return FilingMetadata(
                    ticker=ticker,
                    cik=cik,
                    accession_number=accession_raw,
                    filing_date=filing_date,
                    document_url=doc_url or "",
                )

        logger.warning(f"No 10-K found for {ticker}")
        return None

    def _get_primary_document_url(
        self, cik_padded: str, accession_dashed: str, accession_no_dash: str, ticker: str
    ) -> Optional[str]:
        """
        Gets the URL of the primary 10-K document.

        EDGAR stores multiple documents per filing (exhibits, cover pages, XBRL).
        We fetch the filing's index.json to find the doc explicitly typed "10-K".
        Note: this index lives under www.sec.gov/Archives/..., NOT data.sec.gov.
        """
        index_url = (
            f"{self.ARCHIVES_BASE_URL}/Archives/edgar/data/{int(cik_padded)}/"
            f"{accession_no_dash}/{accession_dashed}-index.json"
        )

        try:
            resp = requests.get(index_url, headers=self.HEADERS, timeout=10)
            resp.raise_for_status()
            index_data = resp.json()
            time.sleep(self.RATE_LIMIT_SLEEP)
        except Exception as e:
            logger.error(f"Index fetch failed for {ticker}: {e}")
            return None

        for doc in index_data.get("documents", []):
            if doc.get("type") == "10-K":
                return (
                    f"{self.ARCHIVES_BASE_URL}/Archives/edgar/data/{int(cik_padded)}/"
                    f"{accession_no_dash}/{doc['name']}"
                )

        logger.warning(f"No primary 10-K document found in index for {ticker}")
        return None

    def download(self, metadata: FilingMetadata) -> Optional[str]:
        """Downloads the filing and saves locally. Returns local path on success."""
        if not metadata.document_url:
            logger.error(f"No document URL for {metadata.ticker}")
            return None

        filename = f"{metadata.ticker}_{metadata.filing_date}_10K.htm"
        local_path = self.output_dir / filename

        if local_path.exists() and local_path.stat().st_size > 500_000:
            logger.info(f"Already downloaded: {filename}")
            metadata.local_path = str(local_path)
            return str(local_path)

        if local_path.exists():
            local_path.unlink()

        try:
            resp = requests.get(metadata.document_url, headers=self.HEADERS, timeout=60)
            resp.raise_for_status()

            if len(resp.content) < 500_000:
                logger.error(
                    f"File too small ({len(resp.content)/1024:.1f} KB) — "
                    f"likely wrong URL for {metadata.ticker}"
                )
                logger.error(f"URL was: {metadata.document_url}")
                return None

            local_path.write_bytes(resp.content)
            logger.success(f"Downloaded {filename} ({len(resp.content)/1024:.1f} KB)")
            metadata.local_path = str(local_path)
            time.sleep(self.RATE_LIMIT_SLEEP)
            return str(local_path)

        except Exception as e:
            logger.error(f"Download failed for {metadata.ticker}: {e}")
            return None
