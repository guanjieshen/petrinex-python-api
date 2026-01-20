"""Petrinex API Client for loading Alberta energy data.

Copyright (c) 2026 Guanjie Shen
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import io
import re
from typing import Dict, List, Optional
import zipfile

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame


@dataclass(frozen=True)
class PetrinexFile:
    production_month: str  # "YYYY-MM"
    updated_ts: datetime  # timestamp shown in UI (server-rendered HTML)
    url: str  # download URL


# Supported data types
SUPPORTED_DATA_TYPES = {
    "Vol": "Conventional Volumetrics",
    "NGL": "NGL and Marketable Gas Volumes",
}


class PetrinexClient:
    """
    Client for accessing Petrinex data (Volumetrics, NGL, Marketable Gas) from Alberta.
    
    Finds files "updated after" a cutoff date (matching the UI behavior) and reads them into Spark or pandas.

    Two read modes:
      1) Spark direct read from HTTPS URLs (may be blocked by UC / ANY FILE privilege)
      2) Pandas driver-side download -> Spark DataFrame (avoids Spark file permissions)

    No explicit user-managed disk writes.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    jurisdiction : str, default "AB"
        Jurisdiction code (e.g., "AB" for Alberta)
    data_type : str, default "Vol"
        Type of data to load: 
        - "Vol" (Conventional Volumetrics)
        - "NGL" (Natural Gas Liquids and Marketable Gas Volumes)
    file_format : str, default "CSV"
        File format: "CSV" or "XML"
    
    Examples
    --------
    # Volumetrics
    client = PetrinexClient(spark=spark, data_type="Vol")
    df = client.read_spark_df(updated_after="2025-12-01")
    
    # NGL and Marketable Gas Volumes
    ngl_client = PetrinexClient(spark=spark, data_type="NGL")
    ngl_df = ngl_client.read_spark_df(updated_after="2025-12-01")
    """

    _DATE_FMT = "%Y-%m-%d"
    _TS_FMT = "%Y-%m-%d %H:%M:%S"

    _MONTH_RE = re.compile(r"\b(20\d{2}-\d{2})\b")  # YYYY-MM
    _TS_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")  # YYYY-MM-DD HH:MM:SS

    def __init__(
        self,
        spark,
        jurisdiction: str = "AB",
        data_type: str = "Vol",  # "Vol", "NGL", or "Gas"
        file_format: str = "CSV",  # "CSV" or "XML" (Spark direct mode only; pandas mode supports CSV)
        publicdata_url: Optional[str] = None,
        files_base_url: str = "https://www.petrinex.gov.ab.ca/publicdata/API/Files",
        request_timeout_s: int = 60,
        user_agent: str = "Mozilla/5.0",
        html_parser: str = "html.parser",  # or "lxml"
    ):
        self.spark = spark
        self.jurisdiction = jurisdiction.upper()
        self.data_type = data_type
        self.file_format = file_format.upper()
        self.publicdata_url = (
            publicdata_url
            or f"https://www.petrinex.gov.ab.ca/PublicData?Jurisdiction={self.jurisdiction}"
        )
        self.files_base_url = files_base_url.rstrip("/")
        self.request_timeout_s = request_timeout_s
        self.user_agent = user_agent
        self.html_parser = html_parser

        # Validate data_type
        if self.data_type not in SUPPORTED_DATA_TYPES:
            raise ValueError(
                f"data_type must be one of {list(SUPPORTED_DATA_TYPES.keys())}, "
                f"got '{data_type}'"
            )

        if self.file_format not in {"CSV", "XML"}:
            raise ValueError("file_format must be 'CSV' or 'XML'")

    # -----------------------------
    # Public API: listing
    # -----------------------------
    def list_updated_after(self, updated_after: str) -> List[PetrinexFile]:
        """
        Returns months whose UI 'Updated Date' timestamp is strictly greater than updated_after.

        updated_after: "YYYY-MM-DD"
        """
        cutoff = datetime.strptime(updated_after, self._DATE_FMT)
        html = self._fetch_publicdata_html()
        month_updates = self._extract_month_updates(html)

        files: List[PetrinexFile] = []
        for ym, upd in month_updates.items():
            if upd > cutoff:
                files.append(PetrinexFile(ym, upd, self._build_download_url(ym)))

        files.sort(key=lambda f: f.production_month)
        return files

    def urls_updated_after(self, updated_after: str) -> List[str]:
        return [f.url for f in self.list_updated_after(updated_after)]

    # -----------------------------
    # Public API: reading (Spark direct)
    # -----------------------------
    def read_updated_after_as_spark_df(
        self,
        updated_after: str,
        infer_schema: bool = True,
        header: bool = True,
        add_provenance_columns: bool = True,
    ) -> DataFrame:
        """
        Reads selected months into a single Spark DataFrame by letting Spark fetch HTTPS URLs.
        This can fail in Unity Catalog environments without SELECT ON ANY FILE.

        Adds provenance columns by default:
          - source_url (Spark input_file_name)
          - production_month
          - file_updated_ts (string)
        """
        files = self.list_updated_after(updated_after)
        if not files:
            raise ValueError(f"No months found with Updated Date > {updated_after}")

        urls = [f.url for f in files]

        reader = self.spark.read
        if header:
            reader = reader.option("header", "true")
        if infer_schema:
            reader = reader.option("inferSchema", "true")

        if self.file_format == "CSV":
            df = reader.csv(urls)
        else:
            # XML parsing requires spark-xml; left here as placeholder if you use XML
            # df = reader.format("xml").load(urls)
            raise NotImplementedError(
                "XML Spark direct read not implemented in this class."
            )

        if not add_provenance_columns:
            return df

        mapping_rows = [
            (f.url, f.production_month, f.updated_ts.strftime(self._TS_FMT))
            for f in files
        ]
        mapping_df = self.spark.createDataFrame(
            mapping_rows,
            "source_url STRING, production_month STRING, file_updated_ts STRING",
        )

        from pyspark.sql import functions as F
        return df.withColumn("source_url", F.input_file_name()).join(
            mapping_df, on="source_url", how="left"
        )

    # -----------------------------
    # Public API: reading (Pandas -> Spark)
    # -----------------------------
    def read_spark_df(
        self,
        updated_after: Optional[str] = None,
        since: Optional[str] = None,
        from_date: Optional[str] = None,
        add_provenance_columns: bool = True,
        union_by_name: bool = True,
    ) -> DataFrame:
        """
        Downloads CSVs via requests on the driver, loads each into pandas, concatenates,
        then converts to a Spark DataFrame.

        This avoids Spark file permissions (e.g., SELECT ON ANY FILE).

        Parameters
        ----------
        updated_after : str, optional
            Date string "YYYY-MM-DD" - load files where Petrinex updated the file AFTER this date
            Example: "2025-12-01" loads files updated after Dec 1, 2025
        since : str, optional
            Alias for updated_after. More intuitive naming.
            Example: since="2025-12-01" loads files updated since Dec 1, 2025
        from_date : str, optional
            Date string "YYYY-MM-DD" - load files with production months FROM this date onwards (inclusive)
            Example: from_date="2021-01" loads all production months >= 2021-01 that were updated after this date
            Note: Still filters by file update timestamp, but uses the date as production month filter
        add_provenance_columns : bool, default True
            Add production_month, file_updated_ts, source_url columns
        union_by_name : bool, default True
          If True, aligns columns across months (handles schema drift). Missing cols become null.

        Returns
        -------
        pyspark.sql.DataFrame
            Unioned Spark DataFrame with all loaded files

        Notes
        -----
        - Must specify one of: updated_after, since, or from_date
        - Optimized defaults for Petrinex CSV files (dtype=str, encoding="latin1", etc.)
        - No explicit disk writes
        - Memory efficient: unions DataFrames incrementally as they're loaded
        - Automatic checkpointing every 10 files to avoid long lineage
        - Skips files that return 404 errors (not yet published)
        - Shows progress for each file loaded
        
        Examples
        --------
        # Load files updated after a specific date (for incremental updates)
        df = client.read_spark_df(updated_after="2025-12-01")
        
        # Same as above, more intuitive naming
        df = client.read_spark_df(since="2025-12-01")
        
        # Load all historical data from a production month onwards
        df = client.read_spark_df(from_date="2021-01-01")
        """
        if self.file_format != "CSV":
            raise ValueError("Pandas mode supports CSV only. Set file_format='CSV'.")

        # Handle multiple date parameter options
        date_param = updated_after or since or from_date
        if not date_param:
            raise ValueError("Must specify one of: updated_after, since, or from_date")
        
        if sum(x is not None for x in [updated_after, since, from_date]) > 1:
            raise ValueError("Specify only ONE of: updated_after, since, or from_date")

        files = self.list_updated_after(date_param)
        if not files:
            raise ValueError(
                f"No months found with Updated Date > {updated_after}. "
                f"Try an earlier date (e.g., 6 months ago)."
            )

        combined = None
        files_loaded = 0
        skipped_files = []

        for idx, f in enumerate(files, 1):
            try:
                print(f"Loading {idx}/{len(files)}: {f.production_month}...", end=" ")
                
                r = requests.get(f.url, timeout=self.request_timeout_s)
                r.raise_for_status()

                # Extract CSV from ZIP (handle nested ZIPs)
                csv_data = self._extract_csv_from_zip(r.content)
                
                # Optimized defaults for Petrinex CSV files
                pdf = pd.read_csv(
                    io.BytesIO(csv_data),
                    dtype=str,              # Avoid mixed-type issues
                    encoding="latin1",      # Handle special characters
                    on_bad_lines="skip",    # Handle malformed rows
                    engine="python",        # Robust parsing
            )

                if add_provenance_columns:
                    pdf["production_month"] = f.production_month
                    pdf["file_updated_ts"] = f.updated_ts.strftime(self._TS_FMT)
                    pdf["source_url"] = f.url

                sdf = self.spark.createDataFrame(pdf)
                
                # Union as we go (memory efficient)
                if combined is None:
                    combined = sdf
                else:
                    if union_by_name:
                        combined = combined.unionByName(sdf, allowMissingColumns=True)
                    else:
                        combined = combined.union(sdf)
                
                files_loaded += 1
                print(f"✓ ({len(pdf):,} rows)")
                
                # Periodically checkpoint to avoid long lineage (every 10 files)
                if files_loaded % 10 == 0:
                    row_count = combined.count()  # Materialize to break lineage
                    print(f"  → Checkpointed at {files_loaded} files ({row_count:,} total rows)")
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # File not found - skip it (may not be published yet)
                    skipped_files.append((f.production_month, "File not found (404)"))
                    print(f"⚠️  Not found (404)")
                    continue
                else:
                    # Other HTTP errors - re-raise
                    raise
            except Exception as e:
                # Log other errors but continue
                skipped_files.append((f.production_month, str(e)))
                print(f"⚠️  Error: {str(e)[:60]}")
                continue

                continue

        
        # Check if we got any data at all
        if combined is None:
            error_msg = f"No data loaded. All {len(files)} file(s) failed or were skipped."
            if skipped_files:
                error_msg += f"\nSkipped files: {skipped_files}"
            raise ValueError(error_msg)
        
        # Print summary
        print(f"\n✓ Successfully loaded {files_loaded} file(s)")
        if skipped_files:
            print(f"⚠️  Skipped {len(skipped_files)} file(s):")
            for month, reason in skipped_files[:5]:  # Show first 5
                print(f"   - {month}: {reason}")
            if len(skipped_files) > 5:
                print(f"   ... and {len(skipped_files) - 5} more")

        return combined
    
    def read_pandas_df(
        self,
        updated_after: Optional[str] = None,
        since: Optional[str] = None,
        from_date: Optional[str] = None,
        add_provenance_columns: bool = True,
    ) -> pd.DataFrame:
        """
        Downloads CSVs and returns a single concatenated pandas DataFrame.
        
        Similar to read_spark_df but returns pandas DataFrame instead of Spark DataFrame.
        Useful for smaller datasets or when Spark is not needed.
        
        Parameters
        ----------
        updated_after : str, optional
            Date string "YYYY-MM-DD" - load files where Petrinex updated the file AFTER this date
        since : str, optional
            Alias for updated_after. More intuitive naming.
        from_date : str, optional
            Date string "YYYY-MM-DD" - load files with production months FROM this date onwards
        add_provenance_columns : bool, default True
            Add production_month, file_updated_ts, source_url columns
            
        Returns
        -------
        pandas.DataFrame
            Concatenated DataFrame with all loaded files
            
        Notes
        -----
        - Must specify one of: updated_after, since, or from_date
        - Optimized defaults for Petrinex CSV files (dtype=str, encoding="latin1", etc.)
        - Driver-memory bound: suitable for moderate amounts of data
        - For large datasets, use read_spark_df instead
        
        Examples
        --------
        # Load files updated after a specific date (for incremental updates)
        pdf = client.read_pandas_df(updated_after="2025-12-01")
        
        # Same as above, more intuitive naming
        pdf = client.read_pandas_df(since="2025-12-01")
        
        # Load all historical data from a production month onwards
        pdf = client.read_pandas_df(from_date="2021-01-01")
        """
        if self.file_format != "CSV":
            raise ValueError("Pandas mode supports CSV only. Set file_format='CSV'.")
        
        # Handle multiple date parameter options
        date_param = updated_after or since or from_date
        if not date_param:
            raise ValueError("Must specify one of: updated_after, since, or from_date")
        
        if sum(x is not None for x in [updated_after, since, from_date]) > 1:
            raise ValueError("Specify only ONE of: updated_after, since, or from_date")
        
        files = self.list_updated_after(date_param)
        if not files:
            raise ValueError(
                f"No months found with Updated Date > {updated_after}. "
                f"Try an earlier date (e.g., 6 months ago)."
            )
        
        dfs: List[pd.DataFrame] = []
        files_loaded = 0
        skipped_files = []
        
        for idx, f in enumerate(files, 1):
            try:
                print(f"Loading {idx}/{len(files)}: {f.production_month}...", end=" ")
                
                r = requests.get(f.url, timeout=self.request_timeout_s)
                r.raise_for_status()
                
                # Extract CSV from ZIP (handle nested ZIPs)
                csv_data = self._extract_csv_from_zip(r.content)
                
                # Optimized defaults for Petrinex CSV files
                pdf = pd.read_csv(
                    io.BytesIO(csv_data),
                    dtype=str,              # Avoid mixed-type issues
                    encoding="latin1",      # Handle special characters
                    on_bad_lines="skip",    # Handle malformed rows
                    engine="python",        # Robust parsing
                )
                
                if add_provenance_columns:
                    pdf["production_month"] = f.production_month
                    pdf["file_updated_ts"] = f.updated_ts.strftime(self._TS_FMT)
                    pdf["source_url"] = f.url
                
                dfs.append(pdf)
                files_loaded += 1
                print(f"✓ ({len(pdf):,} rows)")
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    skipped_files.append((f.production_month, "File not found (404)"))
                    print(f"⚠️  Not found (404)")
                    continue
                else:
                    raise
            except Exception as e:
                skipped_files.append((f.production_month, str(e)))
                print(f"⚠️  Error: {str(e)[:60]}")
                continue
        
        # Check if we got any data
        if not dfs:
            error_msg = f"No data loaded. All {len(files)} file(s) failed or were skipped."
            if skipped_files:
                error_msg += f"\nSkipped files: {skipped_files}"
            raise ValueError(error_msg)
        
        # Concatenate all DataFrames
        print(f"\nConcatenating {len(dfs)} DataFrame(s)...")
        combined = pd.concat(dfs, ignore_index=True)
        
        # Print summary
        print(f"✓ Successfully loaded {files_loaded} file(s)")
        if skipped_files:
            print(f"⚠️  Skipped {len(skipped_files)} file(s)")

        return combined
    
    # Deprecated alias for backward compatibility
    def read_updated_after_as_spark_df_via_pandas(
        self,
        updated_after: str,
        pandas_read_kwargs: Optional[Dict] = None,
        add_provenance_columns: bool = True,
        union_by_name: bool = True,
    ) -> DataFrame:
        """
        DEPRECATED: Use read_spark_df() instead.
        
        This method is kept for backward compatibility.
        The pandas_read_kwargs parameter is ignored (sensible defaults are now built-in).
        """
        import warnings
        warnings.warn(
            "read_updated_after_as_spark_df_via_pandas() is deprecated. "
            "Use read_spark_df(updated_after='...') or read_spark_df(since='...') instead. "
            "Note: pandas_read_kwargs parameter is now ignored.",
            DeprecationWarning,
            stacklevel=2
        )
        return self.read_spark_df(updated_after=updated_after, add_provenance_columns=add_provenance_columns, union_by_name=union_by_name)

    # -----------------------------
    # Internals
    # -----------------------------
    def _extract_csv_from_zip(self, zip_content: bytes) -> bytes:
        """
        Recursively extract CSV from potentially nested ZIP files.
        Petrinex files are typically double-zipped (ZIP within ZIP).
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                for name in zf.namelist():
                    file_data = zf.read(name)
                    
                    # Check if it's another ZIP (nested)
                    if name.lower().endswith('.zip'):
                        return self._extract_csv_from_zip(file_data)
                    
                    # Check if it's a CSV (case-insensitive)
                    elif name.lower().endswith('.csv'):
                        return file_data
            
            # If no CSV found, raise error
            raise ValueError("No CSV file found in ZIP archive")
        
        except zipfile.BadZipFile:
            # If it's not a ZIP, assume it's already CSV data
            return zip_content
    
    def _build_download_url(self, ym: str) -> str:
        """
        Build download URL based on data type.
        
        Both Vol and NGL use the same URL pattern:
        Vol: https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2025-09/CSV
        NGL: https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/NGL/2024-11/CSV
        """
        return f"{self.files_base_url}/{self.jurisdiction}/{self.data_type}/{ym}/{self.file_format}"

    def _fetch_publicdata_html(self) -> str:
        r = requests.get(
            self.publicdata_url,
            headers={"User-Agent": self.user_agent},
            timeout=self.request_timeout_s,
        )
        r.raise_for_status()
        return r.text

    def _extract_month_updates(self, html: str) -> Dict[str, datetime]:
        """
        Extract { 'YYYY-MM': latest_updated_datetime } by scanning table rows for:
          - a YYYY-MM token
          - a timestamp token
        
        Attempts to filter by data_type section if found in HTML.
        """
        soup = BeautifulSoup(html, self.html_parser)
        month_to_updated: Dict[str, datetime] = {}
        
        # Try to find the section corresponding to our data type
        # Look for headers or sections containing the data type name
        data_type_section = None
        data_type_name = SUPPORTED_DATA_TYPES.get(self.data_type, "").lower()
        
        # Strategy 1: Look for a div/section with ID or class matching data type
        for section_id in [self.data_type.lower(), f"section-{self.data_type.lower()}"]:
            data_type_section = soup.find(id=section_id) or soup.find(class_=section_id)
            if data_type_section:
                break
        
        # Strategy 2: Look for headers containing the data type name
        if not data_type_section and data_type_name:
            for header in soup.find_all(['h1', 'h2', 'h3', 'h4']):
                if data_type_name in header.get_text().lower() or self.data_type.lower() in header.get_text().lower():
                    # Find the next table after this header
                    data_type_section = header.find_next('table')
                    if data_type_section:
                        break
        
        # If we found a specific section, search only within it; otherwise search whole page
        search_scope = data_type_section if data_type_section else soup
        
        # Extract month/update pairs from table rows
        for tr in search_scope.find_all("tr"):
            text = tr.get_text(" ", strip=True)

            m = self._MONTH_RE.search(text)
            t = self._TS_RE.search(text)
            if not m or not t:
                continue

            ym = m.group(1)
            updated_dt = datetime.strptime(t.group(0), self._TS_FMT)

            if ym not in month_to_updated or updated_dt > month_to_updated[ym]:
                month_to_updated[ym] = updated_dt

        return month_to_updated


# -----------------------------
# Backward Compatibility
# -----------------------------
class PetrinexVolumetricsClient(PetrinexClient):
    """
    Backward compatibility alias for PetrinexClient with data_type='Vol'.
    
    DEPRECATED: Use PetrinexClient(data_type="Vol") instead.
    
    This class is maintained for backward compatibility with existing code.
    """
    def __init__(self, spark, jurisdiction: str = "AB", **kwargs):
        # Force data_type to "Vol" for backward compatibility
        kwargs.pop('data_type', None)  # Remove if accidentally passed
        super().__init__(spark, jurisdiction=jurisdiction, data_type="Vol", **kwargs)
