from __future__ import annotations

import logging
from datetime import datetime
import os
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple

import keyring
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from email.message import EmailMessage
from lxml import etree
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine
import smtplib

#Check these:
## resolve_bundle_id() => should .bun be included?


# ----------------------------- Status Codes -----------------------------
STATUS_SUCCESS = 0
STATUS_FIN_INVALID = 1
STATUS_ODS_MISSING = 2
STATUS_SOURCE_MISSING = 3
STATUS_XML_INVALID = 4
STATUS_FILE_NOT_FOUND = 5
STATUS_DB_INSERT_ERROR = 6
STATUS_EMAIL_FAILURE = 7
STATUS_UNKNOWN_ERROR = 9


# ----------------------------- Config -----------------------------
@dataclass(frozen=True)
class AppConfig:
    # Servers + port
    db_server_test: str
    db_server_prod: str
    db_port: int

    # Users
    db_user_test: str
    db_user_prod: str
    db_user_prep: str
    db_user_crs: str

    # Keyring services
    keyring_service_test: str
    keyring_service_prod: str
    keyring_service_prep: str
    keyring_service_crs: str

    # Paths
    high_volume_out_dir: str
    lowmid_src_dir: str
    lowmid_dest_dir: str

    # Email
    email_enabled: bool
    smtp_host: str
    smtp_port: int
    email_from: str
    email_cc: List[str]
    email_fallback_to: str

    @staticmethod
    def from_env() -> "AppConfig":
        load_dotenv()

        def must(name: str) -> str:
            v = os.getenv(name)
            if not v:
                raise RuntimeError(f"Missing required env var: {name}")
            return v

        email_enabled = (os.getenv("EMAIL_ENABLED", "false").strip().lower() in {"1", "true", "yes"})

        return AppConfig(
            db_server_test=must("DB_SERVER_TEST"),
            db_server_prod=must("DB_SERVER_PROD"),
            db_port=int(os.getenv("DB_PORT", "1433")),

            db_user_test=must("DB_USER_TEST"),
            db_user_prod=must("DB_USER_PROD"),
            db_user_prep=must("DB_USER_PREP"),
            db_user_crs=must("DB_USER_CRS"),

            keyring_service_test=must("KEYRING_SERVICE_TEST"),
            keyring_service_prod=must("KEYRING_SERVICE_PROD"),
            keyring_service_prep=must("KEYRING_SERVICE_PREP"),
            keyring_service_crs=must("KEYRING_SERVICE_CRS"),

            high_volume_out_dir=must("HIGH_VOLUME_OUT_DIR"),
            lowmid_src_dir=must("LOWMID_SRC_DIR"),
            lowmid_dest_dir=must("LOWMID_DEST_DIR"),

            email_enabled=email_enabled,
            smtp_host=os.getenv("SMTP_HOST", "your_host"),
            smtp_port=int(os.getenv("SMTP_PORT", "port")),
            email_from=os.getenv("EMAIL_FROM", "your_email"),
            email_cc= os.getenv("EMAIL_CC", ""),
            email_fallback_to=os.getenv("EMAIL_FALLBACK_TO", "fallback_person@some_domain.com"),
        )


# ----------------------------- DB Engines -----------------------------
class EngineFactory:
    """
    Creates SQLAlchemy engines for:
      - MCRTracking (tracking)
      - prostate (webplus)
      - TrackDB (PrepPlus bundle registration)
      - PCRegistryMCR (CRS import log)
    """

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg

    def _password(self, service: str, username: str) -> str:
        pwd = keyring.get_password(service, username)
        if not pwd:
            raise RuntimeError(f"Keyring password not found for service='{service}' username='{username}'")
        return pwd

    def _engine(self, server: str, port: int, database: str, username: str, service: str) -> Engine:
        pwd = self._password(service, username)
        conn = (
            f"mssql+pyodbc://{username}:{pwd}@{server}:{port}/{database}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(conn)

    def build(self) -> Dict[str, Engine]:
        return {
            "test": self._engine(
                server=self.cfg.db_server_test,
                port=self.cfg.db_port,
                database="MCRTracking",
                username=self.cfg.db_user_test,
                service=self.cfg.keyring_service_test,
            ),
            "prod": self._engine(
                server=self.cfg.db_server_prod,
                port=self.cfg.db_port,
                database="prostate",
                username=self.cfg.db_user_prod,
                service=self.cfg.keyring_service_prod,
            ),
            "prep": self._engine(
                server=self.cfg.db_server_prod,
                port=self.cfg.db_port,
                database="TrackDB",
                username=self.cfg.db_user_prep,
                service=self.cfg.keyring_service_prep,
            ),
            "crs": self._engine(
                server=self.cfg.db_server_prod,
                port=self.cfg.db_port,
                database="PCRegistryMCR",
                username=self.cfg.db_user_crs,
                service=self.cfg.keyring_service_crs,
            ),
        }


# ----------------------------- Repositories -----------------------------
class TrackingRepo:
    def __init__(self, engine_test: Engine) -> None:
        self.engine_test = engine_test

    def get_last_run_id(self) -> int:
        with self.engine_test.connect() as c:
            row = c.execute(text("SELECT TOP 1 id FROM dbo.last_run ORDER BY id DESC")).fetchone()
        return int(row[0]) if row else 0

    def get_failed_rows(self, max_rows: int = 200) -> List[str]:
        sql = text("""
            SELECT Electronic_File
            FROM dbo.Database_of_XML
            WHERE ISNULL(Process_Status, 0) <> 0
            ORDER BY LastAttemptAt ASC
            OFFSET 0 ROWS FETCH NEXT :n ROWS ONLY
        """)
        with self.engine_test.connect() as c:
            rows = c.execute(sql, {"n": max_rows}).fetchall()
        return [r[0] for r in rows]

    def update_total_tumor_count(self, electronic_file: str, tumor_count: Optional[int]) -> None:
        if tumor_count is None:
            return

        with self.engine_test.begin() as conn:
            conn.execute(
                text("""
                    UPDATE dbo.Database_of_XML
                    SET Total_Tumor_Count = :tumor_count
                    WHERE Electronic_File = :electronic_file
                """),
                {"tumor_count": tumor_count, "electronic_file": electronic_file},
            )

    def upsert_process_status(self, electronic_file: str, status: int, err_msg: Optional[str]) -> None:
        sql_update = text("""
            UPDATE dbo.Database_of_XML
            SET Process_Status = :st,
                Process_Error  = :err,
                LastAttemptAt  = SYSUTCDATETIME()
            WHERE Electronic_File = :ef
        """)
        sql_insert_stub = text("""
            INSERT INTO dbo.Database_of_XML (Electronic_File, Process_Status, Process_Error, LastAttemptAt)
            VALUES (:ef, :st, :err, SYSUTCDATETIME())
        """)
        with self.engine_test.begin() as c:
            res = c.execute(sql_update, {"st": status, "err": (None if not err_msg else err_msg), "ef": electronic_file})
            if res.rowcount == 0:
                c.execute(sql_insert_stub, {"ef": electronic_file, "st": status, "err": (None if not err_msg else err_msg)})

    def insert_if_missing(self, new_row: Dict[str, Any]) -> bool:
        with self.engine_test.connect() as c:
            existing = c.execute(
                text("SELECT COUNT(*) FROM dbo.Database_of_XML WHERE Electronic_File = :ef"),
                {"ef": new_row["Electronic_File"]},
            ).fetchone()[0]
        if int(existing) == 0:
            pd.DataFrame([new_row]).to_sql("Database_of_XML", self.engine_test, if_exists="append", index=False)
            return True
        return False

    def get_tracking_candidates(self) -> List[str]:
        sql = text("""
            SELECT Electronic_File
            FROM dbo.Database_of_XML
            WHERE CRS_Import_Flag IS NULL OR CRS_Import_Flag = 0
        """)
        with self.engine_test.connect() as c:
            rows = c.execute(sql).fetchall()
        return [r[0] for r in rows]

    def update_tracking_from_importlog(self, electronic_file: str, status: Dict[str, Any]) -> None:
        if status.get("updated") == 1:
            sql = text("""
                UPDATE dbo.Database_of_XML
                SET CRS_Import_Flag = :updated,
                    Prepplus_ID = :prep,
                    Date_ODS_Loaded = :dt,
                    Total_Imported_Cases = :tic
                WHERE Electronic_File = :ef
            """)
            params = {
                "updated": status["updated"],
                "prep": status.get("prepPlusBundle"),
                "dt": status.get("dateImported"),
                "tic": status.get("importedCases"),
                "ef": electronic_file,
            }
        else:
            sql = text("""
                UPDATE dbo.Database_of_XML
                SET CRS_Import_Flag = :updated
                WHERE Electronic_File = :ef
            """)
            params = {"updated": status.get("updated", 0), "ef": electronic_file}

        with self.engine_test.begin() as c:
            c.execute(sql, params)

    def update_last_run(self) -> None:
        get_latest_id_query = """
        SELECT TOP 1
        CAST(SUBSTRING([Electronic_File], 2, LEN([Electronic_File]) - 1) AS INT) AS File_Number
        FROM dbo.Database_of_XML
        WHERE Electronic_File LIKE 'F%'
        ORDER BY File_Number DESC;
        """
        with self.engine_test.connect() as c:
            row = c.execute(text(get_latest_id_query)).fetchone()
            last_run_id = row[0] if row else None

        sql_latest_a = """
        SELECT MAX(
            TRY_CONVERT(INT,
                CASE
                  WHEN CHARINDEX('_A', Electronic_File) > 0 THEN
                    CASE
                      WHEN PATINDEX('%[^0-9]%',
                            SUBSTRING(Electronic_File, CHARINDEX('_A', Electronic_File) + 2, 50)) = 0
                        THEN SUBSTRING(Electronic_File, CHARINDEX('_A', Electronic_File) + 2, 50)
                      ELSE LEFT(
                            SUBSTRING(Electronic_File, CHARINDEX('_A', Electronic_File) + 2, 50),
                            PATINDEX('%[^0-9]%',
                              SUBSTRING(Electronic_File, CHARINDEX('_A', Electronic_File) + 2, 50)
                            ) - 1
                           )
                    END
                  ELSE NULL
                END
            )
        ) AS last_a_id
        FROM dbo.Database_of_XML
        WHERE Electronic_File LIKE '%[_]A[0-9]%';
        """
        with self.engine_test.connect() as c:
            ra = c.execute(text(sql_latest_a)).fetchone()
            last_a_id = ra[0] if ra else None

        sql_upsert = text("""
        BEGIN TRANSACTION;
          IF EXISTS (SELECT 1 FROM dbo.last_run)
          BEGIN
            UPDATE dbo.last_run
            SET id = :id,
                last_a_id = COALESCE(:last_a_id, last_a_id);
          END
          ELSE
          BEGIN
            INSERT INTO dbo.last_run (id, last_a_id)
            VALUES (:id, :last_a_id);
          END
        COMMIT;
        """)
        with self.engine_test.connect() as c:
            c.execute(sql_upsert, {"id": last_run_id, "last_a_id": last_a_id})
            c.commit()


class MappingRepo:
    def __init__(self, engine_test: Engine, engine_prod: Engine) -> None:
        self.engine_test = engine_test
        self.engine_prod = engine_prod

    def ods_assignment(self) -> Dict[int, str]:
        with self.engine_test.connect() as c:
            df = pd.read_sql(text("SELECT ODS, Hospital, FIN FROM MCRTracking.dbo.ODS_Assignment"), c)
        df["Hospital"] = df["Hospital"].astype(str).str.strip()
        return df.set_index("FIN")["ODS"].to_dict()

    def email_assignment(self) -> Dict[str, str]:
        with self.engine_test.connect() as c:
            df = pd.read_sql(text("SELECT * FROM MCRTracking.dbo.Email_Assignment"), c)
        df["ODS"] = df["ODS"].astype(str).str.strip()
        df["Email"] = df["Email"].astype(str).str.strip()
        return df.set_index("ODS")["Email"].to_dict()

    def source_map(self) -> Dict[int, str]:
        with self.engine_prod.connect() as c:
            df = pd.read_sql(text("SELECT Label, Value FROM prostate.dbo.UserFacilities"), c)

        df["Label"] = df["Label"].astype(str).str.strip()
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        df = df.dropna(subset=["Value"])
        df["Value"] = df["Value"].astype(int)
        return df.set_index("Value")["Label"].to_dict()


class WebPlusRepo:
    def __init__(self, engine_prod: Engine) -> None:
        self.engine_prod = engine_prod

    def fetch_new_submissions(self, last_run_id: int) -> Tuple[pd.DataFrame, Set[str]]:
        query = text("""
            SELECT BundleID, OriginalFileName, Status, UserID, FacilityID, DateTimeStamp,
                   Comment, Records, HashCode, FileContents, NAACCRVersion, Type, Blobdata, vendor, Tracking
            FROM prostate.dbo.submissions_files
            WHERE Status IN (3,4)
              AND TRY_CAST(
                    LEFT(SUBSTRING(BundleID, PATINDEX('%[0-9]%', BundleID), LEN(BundleID)),
                         PATINDEX('%[^0-9]%', SUBSTRING(BundleID, PATINDEX('%[0-9]%', BundleID), LEN(BundleID))) - 1
                    ) AS INT
                  ) > :last_run_id
            ORDER BY DateTimeStamp DESC;
        """)
        with self.engine_prod.connect() as c:
            df = pd.read_sql(query, c, params={"last_run_id": last_run_id})

        df["BundleID"] = df["BundleID"].astype(str).str.replace(r"\..*$", "", regex=True)

        files: Set[str] = set()
        for _, row in df.iterrows():
            ofn = str(row.get("OriginalFileName", "")).lower()
            if ofn.endswith(".xml"):
                files.add(str(row["BundleID"]))
        return df, files

    def fetch_retry_payloads(self, bases: Set[str]) -> pd.DataFrame:
        if not bases:
            return pd.DataFrame(columns=["BundleID", "FileContents", "FacilityID"])

        sql_retry = text("""
            SELECT BundleID, FileContents, FacilityID
            FROM prostate.dbo.submissions_files
            WHERE (
                CASE WHEN CHARINDEX('.', BundleID) > 0
                     THEN LEFT(BundleID, CHARINDEX('.', BundleID) - 1)
                     ELSE BundleID
                END
            ) IN :bases
        """).bindparams(bindparam("bases", expanding=True))

        with self.engine_prod.connect() as c:
            df = pd.read_sql(sql_retry, c, params={"bases": list(bases)})

        df["BundleID"] = df["BundleID"].astype(str).str.replace(r"\..*$", "", regex=True)
        return df


class PrepPlusRepo:
    def __init__(self, engine_prep: Engine) -> None:
        self.engine_prep = engine_prep

    def resolve_bundle_id(self, electronic_file: str) -> Optional[str]:
        """
        Looks up TrackDB.dbo.Bundle_Registration.BundleId from Bundle_Registration.FileName.

        FileName examples (seen in TrackDB):
          - C:\\RegPlus\\PrepPlus.Net\\RawAbs\\Temp\\F0038163V1.xml
          - ...\\F0038183_V25.xml
          - ...\\F0038193_dac.xml
          - F0000337.bun
          - d070376
        """
        base = re.split(r"[\\/]", electronic_file)[-1]
        base = re.sub(r"\.(xml|bun)$", "", base, flags=re.IGNORECASE)

        like_xml_back = f"%\\{base}%.xml"
        like_xml_slash = f"%/{base}%.xml"
        like_bun_back = f"%\\{base}%.bun"
        like_bun_slash = f"%/{base}%.bun"

        query = text("""
            SELECT TOP 1 BundleId
            FROM dbo.Bundle_Registration
            WHERE FileName LIKE :lx1 OR FileName LIKE :lx2
               OR FileName LIKE :lb1 OR FileName LIKE :lb2
               OR FileName = :exact_base
            ORDER BY BundleId DESC
        """)
        with self.engine_prep.connect() as c:
            row = c.execute(query, {
                "lx1": like_xml_back,
                "lx2": like_xml_slash,
                "lb1": like_bun_back,
                "lb2": like_bun_slash,
                "exact_base": base,
            }).fetchone()

        return str(row[0]) if row else None


class CRSRepo:
    def __init__(self, engine_crs: Engine) -> None:
        self.engine_crs = engine_crs

    def check_import(self, prep_bundle_id: Optional[str]) -> Optional[Dict[str, Any]]:
        if not prep_bundle_id:
            return None

        converted = str(prep_bundle_id).zfill(7)
        pattern_backslash = f"%\\P{converted}.xml"
        pattern_slash = f"%/P{converted}.xml"

        query = text("""
            SELECT TOP 1 *
            FROM PCRegistryMCR.dbo.ImportLog
            WHERE FileName LIKE :pattern_backslash OR FileName LIKE :pattern_slash
            ORDER BY DateImported DESC
        """)

        with self.engine_crs.connect() as c:
            row = c.execute(query, {"pattern_backslash": pattern_backslash, "pattern_slash": pattern_slash}).mappings().fetchone()

        if not row:
            return None

        msg = (row.Message or "").strip().lower()
        pending_cols = [ "PendingTumorLinkage", "PendingConsolidation", "PendingTumorSequence", "PendingPatientLinkage", "PendingDuplicate", "PendingEdit",
                        "PendingTumorSequence_newtumor", "PendingCS", "PendingMType", "PendingTNM", "PendingTNMStageGroupCompare", "Suspense"]

        return {
            "updated": 1 if "import complete" in msg else 0,
            "importID": row.ImportID,
            "importedCases": row.AbsImported,
            "dateImported": row.DateImported,
            "prepPlusBundle": converted,
            "newCaseCount": row.NewCaseCount,
            "disposedAtImport": row.DisposedAtImport,
            "totalPendingFields": sum(row[col] or 0 for col in pending_cols)
        }


# ----------------------------- XML Processing -----------------------------
class XMLProcessor:
    NS = {"n": "http://naaccr.org/naaccrxml"}

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg

    def parse_xml(self, raw_xml: str) -> Optional[str]:
        try:
            parser = etree.XMLParser(remove_blank_text=True)
            root = etree.fromstring(raw_xml.encode("utf-8"), parser)
            for el in root.iter():
                if el.text:
                    el.text = el.text.strip()
                el.tail = None

            tree = etree.ElementTree(root)
            xml_bytes = etree.tostring(tree, encoding="utf-8", xml_declaration=True, pretty_print=False, with_tail=False)
            xml_str = xml_bytes.decode("utf-8")
            xml_str = re.sub(r'>\s*<', '>\n<', xml_str)
            return xml_str
        except Exception:
            return None

    def tumor_count(self, cleaned_xml: str) -> Optional[int]:
        try:
            root = etree.fromstring(cleaned_xml.encode("utf-8"))
            tumor_count = int(root.xpath("count(//n:Patient//n:Tumor)", namespaces=self.NS))
            return tumor_count
        except Exception:
            return None
        

# ----------------------------- Low/Mid File Copier -----------------------------
class LowMidCollector:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg

    def scan_and_copy(self, dry_run: bool = False) -> Tuple[List[Tuple[str, str]], int]:
        """
        Copies low/mid XML files from cfg.lowmid_src_dir to cfg.lowmid_dest_dir and renames to base_A#######.xml
        Returns: (renamed_pairs, max_id_seen)
        """
        src_dir = self.cfg.lowmid_src_dir
        dest_dir = self.cfg.lowmid_dest_dir

        if not os.path.exists(src_dir) or not os.path.exists(dest_dir):
            return [], 0

        files = [f for f in os.listdir(src_dir) if f.lower().endswith(".xml")]
        if not files:
            return [], 0

        pattern = re.compile(r"^(?P<base>.+)_A(?P<num>\d+)\.xml$", re.IGNORECASE)

        max_id = 0
        bases_with_a: Set[str] = set()
        for f in os.listdir(dest_dir):
            if not f.lower().endswith(".xml"):
                continue
            m = pattern.match(f)
            if m:
                bases_with_a.add(m.group("base"))
                max_id = max(max_id, int(m.group("num")))

        renamed: List[Tuple[str, str]] = []
        for f in files:
            if pattern.match(f):
                continue
            base, ext = os.path.splitext(f)
            if base in bases_with_a:
                continue
            max_id += 1
            new_name = f"{base}_A{str(max_id).zfill(7)}{ext}"

            src_path = os.path.join(src_dir, f)
            dest_path = os.path.join(dest_dir, new_name)

            if not dry_run:
                with open(src_path, "rb") as fin, open(dest_path, "wb") as fout:
                    fout.write(fin.read())

            renamed.append((f, new_name))
            bases_with_a.add(base)

        return renamed, max_id


# ----------------------------- Email -----------------------------
class EmailNotifier:
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
    
    def _send(self, to_addr: str, subject: str, body: str, cc: Optional[List[str]] = None) -> None:
        email = EmailMessage()
        email["From"] = self.cfg.email_from
        email["To"] = to_addr
        if cc:
            email["Cc"] = ", ".join(cc)  
        email["Subject"] = subject
        email.set_content(body)
        try:
            with smtplib.SMTP(self.cfg.smtp_host, port=self.cfg.smtp_port) as smtp:
                smtp.send_message(email)
        except Exception as e:
            self.logger.exception("Failed to send email: %s", e)



    def send_assignment(self, to_addr: str, subject: str, body: str) -> None:
        self._send(
            to_addr=to_addr,
            subject=subject,
            body=body,
            cc=self.cfg.email_cc
        )
    

    def invalid_non_hospital_error(self) -> None:
        subject = "Unexpected error processing the low-mid volume files"
        body = (
            "Hey, there was an unexpected error while reading the low/mid volume files from the source and putting them into destination."
            "\n\nRegards,\nMCR Tracker"
        )
        self._send(
            to_addr="stulgos@health.missouri.edu",
            subject=subject,
            body=body,
            cc=self.cfg.email_cc,
        )

    def send_missing_ods_email(self, data_fno: str, fin: str, ods: str) -> None:
        subject = f"ERROR IN File: {data_fno}"
        body = (
            f"Hey,\n\nThe FIN NUMBER: {fin} does not match with any ODS for the record {data_fno}."
            f"Metadata: File: {data_fno}\n FIN Number: {fin}\n Recordeed ODS: {ods}"
            f"\n\nRegards,\nMCR Tracker"
        )
        self._send(
            to_addr="stulgos@health.missouri.edu",
            subject=subject,
            body=body,
            cc=self.cfg.email_cc,
        )

    def warn_finno_mismatch(self, data_fno: str, data_finno: int, webplus_facilityID: int) -> None:
        subject = f"Mismatch for the electronic file: {data_fno}; mismatch in FIN number"
        body = (
            f"Hey,\n\nThe FIN Number assigned for the elctronic file-{data_fno} in the Webplus database is {webplus_facilityID}."
            f"However, the FIN number in {data_fno}.xml xml file is-{data_finno}. Continuing the process with {webplus_facilityID} as file number"
            f"\n\nRegards,\nMCR Tracker"
        )
        self._send(
            to_addr="stulgos@health.missouri.edu",
            subject=subject,
            body=body,
            cc=self.cfg.email_cc,
        )

# ----------------------------- Pipeline -----------------------------
class MCRPipeline:
    def __init__(self, cfg: AppConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.logger =  logger
        engines = EngineFactory(cfg).build()
        self.eng_test = engines["test"]
        self.eng_prod = engines["prod"]
        self.eng_prep = engines["prep"]
        self.eng_crs = engines["crs"]

        self.tracking = TrackingRepo(self.eng_test)
        self.mappings = MappingRepo(self.eng_test, self.eng_prod)
        self.webplus = WebPlusRepo(self.eng_prod)
        self.prepplus = PrepPlusRepo(self.eng_prep)
        self.crs = CRSRepo(self.eng_crs)
        self.xmlp = XMLProcessor(cfg)
        self.lowmid = LowMidCollector(cfg)
        self.emailer = EmailNotifier(cfg, logger)

        self.ods_assignment = self.mappings.ods_assignment()
        self.email_assignment = self.mappings.email_assignment()
        self.source_map = self.mappings.source_map()

    def _extract_patient_records_from_webplus(self, df_webplus: pd.DataFrame, bundle_id: str) -> Tuple[Optional[List[Any]], Optional[int], Optional[str]]:
        data = df_webplus.loc[df_webplus["BundleID"] == bundle_id, "FileContents"].values
        if len(data) == 0 or data[0] is None or len(str(data[0]).strip()) == 0:
            return None, None, None

        raw_xml = str(data[0])
        cleaned = self.xmlp.parse_xml(raw_xml)
        if cleaned is None:
            self.tracking.upsert_process_status(bundle_id, STATUS_XML_INVALID, "XML parsing failed")
            return None, None, None

        tumor_count = self.xmlp.tumor_count(cleaned)
        self.tracking.update_total_tumor_count(bundle_id, tumor_count)

        # write high-volume output
        os.makedirs(self.cfg.high_volume_out_dir, exist_ok=True)
        out_file = os.path.join(self.cfg.high_volume_out_dir, f"{bundle_id}.xml")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(cleaned)

        bs = BeautifulSoup(raw_xml, "xml")
        return bs.find_all("Patient"), tumor_count, cleaned

    def _extract_patient_records_from_lowmid(self, bundle_filename: str) -> Tuple[Optional[List[Any]], Optional[int], Optional[str]]:
        xml_path = os.path.join(self.cfg.lowmid_dest_dir, bundle_filename)
        if not os.path.exists(xml_path):
            return None, None, None

        with open(xml_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_xml = f.read().strip()
        if not raw_xml:
            return None, None, None

        cleaned = self.xmlp.parse_xml(raw_xml)
        if cleaned is None:
            self.tracking.upsert_process_status(bundle_filename.replace(".xml",""), STATUS_XML_INVALID, "XML parsing failed")
            return None, None, None

        tumor_count = self.xmlp.tumor_count(cleaned)
        ef = bundle_filename[:-4] if bundle_filename.lower().endswith(".xml") else bundle_filename
        self.tracking.update_total_tumor_count(ef, tumor_count)
        bs = BeautifulSoup(raw_xml, "xml")
        return bs.find_all("Patient"), tumor_count, cleaned

    def _extract_bundle(self, bundle_id: str, df_webplus: Optional[pd.DataFrame]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Returns (row_dict_for_insert, error_message)
        """
        patients = None
        tumor_count = None

        # High volume (WebPlus df)
        if df_webplus is not None and (df_webplus["BundleID"] == bundle_id).any():
            patients, tumor_count, _ = self._extract_patient_records_from_webplus(df_webplus, bundle_id)

        # Low/mid fallback (file on disk)
        if patients is None:
            patients, tumor_count, _ = self._extract_patient_records_from_lowmid(f"{bundle_id}.xml" if not bundle_id.lower().endswith(".xml") else bundle_id)

        if patients is None:
            return None, "XML invalid or no <Patient> nodes"

        # FIN
        try:
            fin = int(
                patients[0].find("Tumor").find("Item", attrs={"naaccrId": "reportingFacility"}).text
            )
        except Exception:
            fin = 0

        try:
            if df_webplus is not None and not df_webplus.empty and (df_webplus['BundleID'] == bundle_id).any():
                webplus_finno:int = int(df_webplus.loc[df_webplus['BundleID'] == bundle_id, 'FacilityID'].iloc[0])
            else:
                webplus_finno = fin
        except Exception:
            webplus_finno:int = fin

        if webplus_finno!=fin:
            self.emailer.warn_finno_mismatch(os.path.splitext(bundle_id)[0], fin, webplus_finno)
        # Year buckets
        data_per_year: Dict[int, int] = {}
        for record in patients:
            tumor_attr = record.find("Tumor")

            try:
                dod_raw = tumor_attr.find("Item", attrs={"naaccrId": "dateOfDiagnosis"}).text
                dod = int(dod_raw[:4]) if len(dod_raw) == 8 else 0
            except Exception:
                dod = 0


            data_per_year[dod] = data_per_year.get(dod, 0) + 1

        ods = self.ods_assignment.get(webplus_finno, np.nan)
        source = self.source_map.get(webplus_finno, np.nan)

        # CRS import / importedCases (best effort)
        imported_cases = 0
        prep_bundle_id = self.prepplus.resolve_bundle_id(bundle_id)
        status = self.crs.check_import(prep_bundle_id)
        if status and status.get("updated") == 1 and status.get("importedCases") is not None:
            imported_cases = int(status["importedCases"])

        row: Dict[str, Any] = {
            "Electronic_File": bundle_id,
            "FIN_NUMBER": webplus_finno,
            "Total_Counted_Cases": len(patients),
            "Total_Tumor_Count": tumor_count,          
            "Total_Imported_Cases": imported_cases,   
            "Date_Processed": date.today(),
            "Assigned_ODS": ods,
            "Source_Name": source,
            "Error_Year": 0,
            "CRS_Import_Flag": None,
        }

        # Initialize year columns
        for yr in range(2000, date.today().year + 1):
            row[str(yr)] = 0

        # Fill year counts
        for yr, cnt in data_per_year.items():
            if yr != 0:
                row[str(yr)] = cnt
            else:
                row["Error_Year"] = cnt

        return row, None

    def process_files(self, df_webplus: Optional[pd.DataFrame], bundle_ids: Set[str], lowmid_renamed: List[Tuple[str, str]]) -> None:
        lowmid_ids = set()
        for _, new_name in lowmid_renamed or []:
            if os.path.exists(os.path.join(self.cfg.lowmid_dest_dir, new_name)):
                lowmid_ids.add(new_name.replace(".xml", ""))  # store base

        all_ids = list(bundle_ids or []) + list(lowmid_ids or [])

        for bundle_id in all_ids:
            try:
                row, err = self._extract_bundle(bundle_id, df_webplus)
                if row is None:
                    self.tracking.upsert_process_status(bundle_id, STATUS_XML_INVALID, err or "Extraction failed")
                    continue

                self.tracking.insert_if_missing(row)
                self.tracking.upsert_process_status(bundle_id, STATUS_SUCCESS, None)

                
                if self.cfg.email_enabled:
                    fin = row.get("FIN_NUMBER", 0)
                    ods = row.get("Assigned_ODS", None)

                    if fin in (None, 0):
                        self.tracking.upsert_process_status(bundle_id, STATUS_FIN_INVALID, "FIN missing/invalid")
                        self.emailer.send_missing_ods_email(bundle_id, fin, ods)
                        continue
                    if ods is None or (isinstance(ods, float) and np.isnan(ods)):
                        self.tracking.upsert_process_status(bundle_id, STATUS_ODS_MISSING, f"ODS missing for FIN {fin}")
                        self.emailer.send_missing_ods_email(bundle_id, fin, ods)
                        continue
                    if str(ods) not in self.email_assignment:
                        self.tracking.upsert_process_status(bundle_id, STATUS_ODS_MISSING, f"No email mapping for ODS '{ods}' (FIN {fin})")
                        self.emailer.send_missing_ods_email(bundle_id, fin, ods)
                        continue

                    # You can change what you show in email here:
                    subject = f"New Bundle File - {bundle_id} has been Assigned to You"
                    body = (
                        f"Hey,\n\n"
                        f"File Name: {bundle_id}\n\n"
                        f"FIN NUMBER: {fin}\n"
                        f"Source Name: {row.get('Source_Name')}\n"
                        f"Total Tumor Count: {row.get('Total_Tumor_Count')}\n"
                        f"Total Imported Cases: {row.get('Total_Imported_Cases')}\n\n"
                        f"Regards,\nMCR Tracker"
                    )
                    self.emailer.send_assignment(self.email_assignment[str(ods)], subject, body)

            except Exception as e:
                self.tracking.upsert_process_status(bundle_id, STATUS_UNKNOWN_ERROR, f"Unhandled: {e}")

    def run_update_tracking(self) -> None:
        candidates = self.tracking.get_tracking_candidates()
        for ef in candidates:
            try:
                prep_bundle_id = self.prepplus.resolve_bundle_id(ef)
                status = self.crs.check_import(prep_bundle_id)
                if not status:
                    continue
                self.tracking.update_tracking_from_importlog(ef, status)
            except Exception:
                continue

    def run(self) -> None:
        # Retry failed first
        failed = self.tracking.get_failed_rows()
        self.logger.info("Failed rows to retry: %d", len(failed))
        if failed:
            retry_bases: Set[str] = set()
            for ef in failed:
                base = ef[:-4] if ef.lower().endswith(".xml") else ef
                retry_bases.add(base)

            df_retry = self.webplus.fetch_retry_payloads(retry_bases)
            self.process_files(df_retry, retry_bases, lowmid_renamed=[])

        # Low/mid copy step
        try:
            renamed, _ = self.lowmid.scan_and_copy(dry_run=False)
            self.logger.info("Low/mid renamed copied: %d", len(renamed))
        except Exception as e:
            self.logger.exception("Low/mid copy failed: %s", e)
            if self.cfg.email_enabled:
                self.emailer.invalid_non_hospital_error()
            renamed = []

        # New submissions
        last_run_id = self.tracking.get_last_run_id()
        self.logger.info("last_run_id = %d", last_run_id)

        df_webplus, files = self.webplus.fetch_new_submissions(last_run_id)
        self.logger.info("WebPlus rows fetched: %d | unique bundle IDs: %d", len(df_webplus), len(files))
        self.logger.debug("Sample bundle IDs: %s", list(sorted(files))[:10])

        self.process_files(df_webplus, files, renamed)

        # Update tracking from CRS log
        self.logger.info("Updating CRS import flags")
        self.run_update_tracking()

        # Checkpoint
        self.logger.info("Updating checkpoint (dbo.last_run)")
        self.tracking.update_last_run()

        self.logger.info("Pipeline run complete")

def get_logger(name: str = "mcr_tracker", log_dir: str = "logs", level: int = logging.INFO) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{name}.log")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # File handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info("Logger initialized -> %s", log_path)
    return logger

def main() -> None:
    logger = get_logger(level=logging.DEBUG) 
    logger.info("Initiating the Process...")
    cfg = AppConfig.from_env()
    pipeline = MCRPipeline(cfg, logger)
    pipeline.run()


if __name__ == "__main__":
    main()
