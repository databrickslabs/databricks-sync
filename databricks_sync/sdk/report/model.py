import base64
import datetime
import os
import traceback
import uuid
from pathlib import Path
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, Column, String, DateTime, UniqueConstraint, and_, update, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from styleframe import StyleFrame, utils, Styler
from tabulate import tabulate

from databricks_sync import log
from databricks_sync.sdk.sync.constants import EnvVarConstants

driver = os.environ.get(EnvVarConstants.DATABRICKS_SYNC_REPORT_DB_DRIVER, "sqlite")
path = os.environ.get(EnvVarConstants.DATABRICKS_SYNC_REPORT_DB_URL,
                      None)
echo = os.environ.get(EnvVarConstants.DATABRICKS_SYNC_REPORT_DB_TRACE,
                      "false")
if path is None:
    default_db_path = Path.home() / '.databricks_sync/'
    default_db_path.mkdir(parents=True, exist_ok=True)
    default_db_name = "report.db"
    path = str(default_db_path / default_db_name)

db_connection_string = f'{driver}:///{path}'

if echo.upper() == "TRUE":
    engine = create_engine(db_connection_string, echo=True)
else:
    engine = create_engine(db_connection_string)

Base = declarative_base()


class ReportRecord(Base):
    __tablename__ = 'report_records'

    id = Column(String, primary_key=True)
    run_id = Column(String)
    workspace_url = Column(String)
    start_ts = Column(DateTime)
    end_ts = Column(DateTime)
    object_id = Column(String)
    api_object_id = Column(String)
    object_name = Column(String)
    object_type = Column(String)
    status = Column(String)
    file_path = Column(String)
    error_msg = Column(String)
    validation_msg = Column(String)
    error_traceback = Column(String)
    validation_traceback = Column(String)
    __table_args__ = (UniqueConstraint("run_id", "object_id", "object_type", name="_unique_record"),)

    def __repr__(self):
        return "test"


class DBManager:

    def __init__(self, run_id, session):
        self._session = session
        self.__run_id = run_id

    @property
    def run_id(self):
        return self.__run_id


class EventManager(DBManager):

    def get_record_id(self, workspace_url, object_id, object_type):
        return f"{workspace_url}-{self.run_id}-{object_id}-{object_type}"

    def make_start_record(self, workspace_url, object_id, object_type, api_object_id, human_readable_name):
        record = ReportRecord(
            id=self.get_record_id(workspace_url, object_id, object_type),
            run_id=self.run_id,
            workspace_url=workspace_url,
            start_ts=datetime.datetime.utcnow(),
            object_id=object_id,
            api_object_id=api_object_id,
            object_name=human_readable_name,
            object_type=object_type,
            status="STARTED")
        self._session.add(record)
        self._session.commit()

    def make_end_record(self, workspace_url, object_id, object_type, status, errors: Optional[List[Exception]] = None,
                        file_path=None):
        for record in self._session.query(ReportRecord) \
                .filter(ReportRecord.id == self.get_record_id(workspace_url, object_id, object_type)).all():
            record: ReportRecord
            record.end_ts = datetime.datetime.utcnow()
            record.status = status
            if errors is not None and len(errors) > 0:
                error_list = []
                for err in errors:
                    error_list.append("\n".join(traceback.format_exception(type(err), err, err.__traceback__)))
                record.error_msg = "\n".join([f"{type(err)}: {str(err)}" for err in errors])
                record.error_traceback = "\n".join(error_list)
            record.file_path = file_path
            self._session.commit()

    def make_validation_records(self, workspace_url, paths: List[str], validation_msg_list: List[str],
                                validation_traceback_list: List[str]):
        for path, validation_msg, validation_tb in zip(paths, validation_msg_list, validation_traceback_list):
            for record in self._session.query(ReportRecord) \
                    .filter(ReportRecord.run_id == self.run_id,
                            ReportRecord.workspace_url == workspace_url,
                            ReportRecord.file_path.like(f"%{path}%")) \
                    .all():
                record: ReportRecord
                record.validation_msg = validation_msg
                record.validation_traceback = validation_tb
                self._session.commit()

        # Update rest stmt
        stmt = update(ReportRecord) \
            .where(and_(ReportRecord.run_id == self.run_id,
                        ReportRecord.workspace_url == workspace_url,
                        ReportRecord.validation_msg == None,
                        ReportRecord.validation_traceback == None,
                        ReportRecord.error_msg == None,
                        ReportRecord.error_traceback == None,
                        ReportRecord.status == ReportConstants.OBJECT_EXPORT_SUCCEEDED)) \
            .values(validation_msg=ReportConstants.OBJECT_VALIDATION_PASSED,
                    validation_traceback=ReportConstants.OBJECT_VALIDATION_PASSED)
        self._session.execute(stmt)
        self._session.commit()

class ReportConstants:
    OBJECT_EXPORT_SUCCEEDED = "SUCCEEDED"
    OBJECT_EXPORT_ERROR = "EXPORT ERROR"
    OBJECT_VALIDATION_ERROR = "VALIDATION ERROR"
    OBJECT_VALIDATION_PASSED = "PASSED"

class ReportManager(DBManager):

    def __init__(self, run_id, session):
        super().__init__(run_id, session)
        self.run_summary: Optional[pd.DataFrame] = None
        self.run_errors: Optional[pd.DataFrame] = None
        self.run_results: Optional[pd.DataFrame] = None
        self.run_errors_summary: Optional[pd.DataFrame] = None

    def fetch_and_gather_results(self, workspace_url):
        self.run_summary = self.__get_run_summary_df(workspace_url)
        self.run_errors = self.__get_run_errors_df(workspace_url)
        self.run_errors_summary = self.__get_run_errors_summary_df(workspace_url)
        self.run_results = self.__get_run_results(workspace_url)
        return self

    def print_to_xlsx(self):
        file_name = f"{self.run_id}_report.xlsx"
        log.debug(f"Writing to Excel File: {file_name}")
        style = Styler(horizontal_alignment=utils.horizontal_alignments.left,
                       vertical_alignment=utils.vertical_alignments.top)
        writer = StyleFrame.ExcelWriter(file_name)
        if self.run_summary is not None and not self.run_summary.empty:
            StyleFrame(self.run_summary, style) \
                .to_excel(writer,
                          sheet_name="run summary",
                          row_to_add_filters=0,
                          columns_and_rows_to_freeze='A2')
        if self.run_errors_summary is not None and not self.run_errors_summary.empty:
            StyleFrame(self.run_errors_summary, style) \
                .to_excel(writer,
                          sheet_name="run errors summary",
                          row_to_add_filters=0,
                          columns_and_rows_to_freeze='A2')
        if self.run_results is not None and not self.run_results.empty:
            StyleFrame(self.run_results, style) \
                .to_excel(writer,
                          sheet_name="run results",
                          row_to_add_filters=0,
                          columns_and_rows_to_freeze='A2')
        writer.save()
        writer.close()

    def print_to_console(self):
        log.info(f"Run Id: {self.run_id}")
        log.info("Run Summary:")
        log.info("============")
        log.info("\n" + tabulate(self.run_summary, headers='keys', tablefmt='fancy_grid'))
        log.info("")
        log.info("Run Errors Summary:")
        log.info("===================")
        log.info("\n" + tabulate(self.run_errors_summary, headers='keys', tablefmt='fancy_grid'))
        return self

    def __get_run_summary_df(self, workspace_url):
        stmt = text(f"""
                    SELECT object_type,
                           SUM(CASE WHEN status='{ReportConstants.OBJECT_EXPORT_SUCCEEDED}' THEN 1 ELSE 0 END) 
                                as export_succeeded,
                           SUM(CASE WHEN status='{ReportConstants.OBJECT_EXPORT_SUCCEEDED}' THEN 0 ELSE 1 END) 
                                as export_failed,
                           SUM(CASE WHEN validation_msg='{ReportConstants.OBJECT_VALIDATION_PASSED}' THEN 1 ELSE 0 END) 
                                as validate_succeeded,
                           SUM(CASE WHEN validation_msg='{ReportConstants.OBJECT_VALIDATION_PASSED}' THEN 0 ELSE 1 END) 
                                as validate_failed
                    FROM report_records
                    WHERE run_id=:run_id
                    AND workspace_url=:workspace_url
                    GROUP BY workspace_url, run_id, object_type
                    ORDER BY object_type
                """)
        return pd.read_sql(self._session.query(ReportRecord)
                           .from_statement(stmt)
                           .params(run_id=self.run_id,
                                   workspace_url=workspace_url)
                           .statement,
                           self._session.bind)

    def __get_run_results(self, workspace_url):
        stmt = text("""
                    SELECT start_ts, end_ts, object_id, object_type, object_name, status, file_path, error_msg, validation_msg, 
                           error_traceback, validation_traceback
                    FROM report_records
                    WHERE run_id=:run_id
                    AND workspace_url=:workspace_url
                """)
        return pd.read_sql(self._session.query(ReportRecord)
                           .from_statement(stmt)
                           .params(run_id=self.run_id,
                                   workspace_url=workspace_url)
                           .statement,
                           self._session.bind)

    def __get_run_errors_summary_df(self, workspace_url):
        stmt = text(f""" 
                    SELECT '{ReportConstants.OBJECT_EXPORT_ERROR}' as err_type, error_msg as msg, count(1) as err_count
                    FROM report_records
                    WHERE run_id=:run_id
                        AND workspace_url=:workspace_url
                        AND (error_msg is not NULL AND error_msg != '{ReportConstants.OBJECT_VALIDATION_PASSED}')
                    GROUP BY error_msg, '{ReportConstants.OBJECT_EXPORT_ERROR}'
                    UNION
                    SELECT '{ReportConstants.OBJECT_EXPORT_ERROR}' as err_type, validation_msg as msg, count(1) 
                        as err_count
                    FROM report_records
                    WHERE run_id=:run_id
                        AND workspace_url=:workspace_url
                        AND (
                            validation_msg is not NULL 
                            AND validation_msg != '{ReportConstants.OBJECT_VALIDATION_PASSED}'
                        )
                    GROUP BY validation_msg, '{ReportConstants.OBJECT_EXPORT_ERROR}'
                """)
        return pd.read_sql(self._session.query(ReportRecord)
                           .from_statement(stmt)
                           .params(run_id=self.run_id,
                                   workspace_url=workspace_url)
                           .statement,
                           self._session.bind)

    def __get_run_errors_df(self, workspace_url, encoded=False):
        stmt = text(f"""
                    SELECT workspace_url, run_id, start_ts, end_ts, object_type, object_id, file_path, error_msg, 
                        validation_msg, error_traceback, validation_traceback
                    FROM report_records
                    WHERE run_id=:run_id
                    AND workspace_url=:workspace_url
                    AND (error_msg is not NULL OR validation_msg != '{ReportConstants.OBJECT_VALIDATION_PASSED}')
                """)
        summary = pd.read_sql(self._session.query(ReportRecord)
                              .from_statement(stmt)
                              .params(run_id=self.run_id,
                                      workspace_url=workspace_url)
                              .statement,
                              self._session.bind)
        if encoded is False:
            return summary
        else:
            summary["error_traceback"] = summary.error_msg.str.encode('utf-8', 'strict').apply(base64.b64encode)
            summary["validation_traceback"] = summary.validation.str.encode('utf-8', 'strict').apply(base64.b64encode)
            summary["error_traceback"] = summary.error_msg.str.decode('utf-8', 'strict')
            summary["validation_traceback"] = summary.validation.str.encode('utf-8', 'strict')
            return summary


Base.metadata.create_all(engine)
# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()
run_id = str(uuid.uuid4())
event_manager = EventManager(run_id=run_id, session=session)
report_manager = ReportManager(run_id=run_id, session=session)
