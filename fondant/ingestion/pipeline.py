from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import structlog
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.db.models import (
    IMPERR,
    IMPLOG,
    SECDIV,
    SECMDA,
    SOURCEAGE,
    SOURCERAW,
    SOURCERPT,
    TAXADJ,
    TAXCAT,
    TAXCOR,
    TAXDAT,
    TAXLIN,
    TAXRPT,
)
from fondant.db.session import AsyncSessionFactory
from fondant.oekb.client import OeKBClient
from fondant.oekb.models import OeKBReportListItem
from fondant.oekb.parser import (
    CATEGORY_CODE_BY_KEY,
    METRIC_CODE_BY_KEY,
    build_sourceage_values,
    build_sourceraw_values,
    build_sourcerpt_values,
)

logger = structlog.get_logger(__name__)

LINE_DICTIONARY: list[dict[str, object]] = [
    {"line_code": "K40", "metric_key": "steuerpflichtige_einkuenfte", "name_de": "Steuerpflichtige Einkuenfte", "name_en": "taxable_income", "line_order": 5},
    {"line_code": "K11", "metric_key": "ag_ertraege", "name_de": "AGErtraege", "name_en": "distributed_income", "line_order": 10},
    {"line_code": "K12", "metric_key": "korrekturbetrag_saldiert", "name_de": "Korrekturbetrag saldiert", "name_en": "net_correction_amount", "line_order": 20},
    {"line_code": "K81", "metric_key": "kest_total", "name_de": "KESt gesamt", "name_en": "withholding_tax_total", "line_order": 30},
    {"line_code": "K82", "metric_key": "kest_substanzgewinne", "name_de": "KESt Substanzgewinne", "name_en": "withholding_tax_substance_gains", "line_order": 40},
    {"line_code": "K10", "metric_key": "substanzgewinne_kestpfl", "name_de": "Substanzgewinne KESt-pflichtig", "name_en": "taxable_substance_gains", "line_order": 50},
    {"line_code": "K55", "metric_key": "fondsergebnis_nichtausg", "name_de": "Fondsergebnis nicht ausgeschuettet", "name_en": "undistributed_fund_result", "line_order": 60},
    {"line_code": "K61", "metric_key": "korrekturbetrag_age_ak", "name_de": "Korrekturbetrag Anschaffungskosten", "name_en": "cost_basis_adjustment", "line_order": 70},
    {"line_code": "K62", "metric_key": "korrekturbetrag_aussch_ak", "name_de": "Korrekturbetrag Ausschuettung Anschaffungskosten", "name_en": "distribution_cost_basis_adjustment", "line_order": 75},
    {"line_code": "K36", "metric_key": "substanzgew_folgejahre", "name_de": "Substanzgewinn Folgejahre", "name_en": "taxable_substance_gain_followup_years", "line_order": 80},
    {"line_code": "K21", "metric_key": "quellensteuern_einbeh", "name_de": "Quellensteuern einbehalten", "name_en": "withholding_taxes_retained", "line_order": 90},
]

CATEGORY_DICTIONARY: list[dict[str, object]] = [
    {"category_code": "PVM", "category_key": "pv_mit", "name_de": "Privatvermoegen mit Option", "name_en": "private_assets_with_option", "category_order": 10},
    {"category_code": "PVO", "category_key": "pv_ohne", "name_de": "Privatvermoegen ohne Option", "name_en": "private_assets_without_option", "category_order": 20},
    {"category_code": "BVM", "category_key": "bv_mit", "name_de": "Betriebsvermoegen mit Option", "name_en": "business_assets_with_option", "category_order": 30},
    {"category_code": "BVO", "category_key": "bv_ohne", "name_de": "Betriebsvermoegen ohne Option", "name_en": "business_assets_without_option", "category_order": 40},
    {"category_code": "BVJ", "category_key": "bv_jur", "name_de": "Betriebsvermoegen juristisch", "name_en": "business_assets_legal_entities", "category_order": 50},
    {"category_code": "STF", "category_key": "stiftung", "name_de": "Stiftung", "name_en": "foundation", "category_order": 60},
]


@dataclass(slots=True)
class IngestionResult:
    isin: str
    status: str
    records_seen: int
    records_written: int
    run_id: uuid.UUID
    message: str | None = None


async def ingest_isin(isin: str, *, client: OeKBClient | None = None) -> IngestionResult:
    run_id = uuid.uuid4()
    owned_client = client is None
    async with AsyncSessionFactory() as session:
        log_entry = await _log_run_started(session=session, run_id=run_id, isin=isin)
        records_seen = 0
        records_written = 0
        records_skipped = 0

        try:
            if client is None:
                client = await OeKBClient().__aenter__()

            await _ensure_tax_dictionaries(session=session)
            reports = await client.get_report_list(isin)
            await _upsert_security_master(session=session, isin=isin, reports=reports)

            final_reports = [report for report in reports if report.status_code == "FIN"]
            records_seen = len(final_reports)

            for report in reports:
                detail = await client.get_report_detail(report.stm_id) if report.status_code == "FIN" else None
                sourcerpt_values = build_sourcerpt_values(isin=isin, report=report, detail=detail)
                await _upsert(
                    session,
                    SOURCERPT,
                    sourcerpt_values,
                    [SOURCERPT.isin, SOURCERPT.stm_id],
                    preserve_existing_on_null=True,
                    allow_equal_version_update=True,
                )

                if report.status_code != "FIN" or detail is None:
                    continue

                sourceraw_values = build_sourceraw_values(isin=isin, report=report, detail=detail)
                should_persist, allow_equal_version_update = await _should_persist_source_report(
                    session=session,
                    isin=isin,
                    stm_id=report.stm_id,
                    incoming_version=sourceraw_values["versions_nr"],
                    incoming_payload=sourceraw_values["payload"],
                )
                if not should_persist:
                    records_skipped += 1
                    continue

                sourceage_values = build_sourceage_values(isin=isin, report=report, detail=detail)
                sourceage_values["report_year"] = sourcerpt_values.get("report_year")

                await _upsert(
                    session,
                    SOURCEAGE,
                    sourceage_values,
                    [SOURCEAGE.isin, SOURCEAGE.stm_id],
                    allow_equal_version_update=allow_equal_version_update,
                    preserve_existing_on_null=True,
                )
                await _upsert(
                    session,
                    SOURCERAW,
                    sourceraw_values,
                    [SOURCERAW.isin, SOURCERAW.stm_id],
                    allow_equal_version_update=allow_equal_version_update,
                )

                await _curate_report(session=session, isin=isin, stm_id=report.stm_id)
                records_written += 1

            log_entry.status = "SUCCESS"
            log_entry.records_seen = records_seen
            log_entry.records_written = records_written
            log_entry.finished_at = datetime.now(timezone.utc)
            log_entry.message = (
                f"Processed {records_seen} FIN reports; wrote {records_written}; "
                f"skipped {records_skipped} unchanged/older reports"
            )
            await session.commit()

            logger.info(
                "ingestion.success",
                isin=isin,
                run_id=str(run_id),
                records_seen=records_seen,
                records_written=records_written,
                records_skipped=records_skipped,
            )
            return IngestionResult(
                isin=isin,
                status="SUCCESS",
                run_id=run_id,
                records_seen=records_seen,
                records_written=records_written,
                message=log_entry.message,
            )

        except Exception as exc:
            await session.rollback()
            error_message = str(exc)
            session.add(
                IMPERR(
                    run_id=run_id,
                    isin=isin,
                    stage="ingestion",
                    error_code=type(exc).__name__,
                    error_message=error_message,
                    payload=None,
                )
            )
            log_entry.status = "FAILED"
            log_entry.records_seen = records_seen
            log_entry.records_written = records_written
            log_entry.finished_at = datetime.now(timezone.utc)
            log_entry.message = error_message
            session.add(log_entry)
            await session.commit()

            try:
                logger.exception("ingestion.failed", isin=isin, run_id=str(run_id))
            except UnicodeEncodeError:
                logger.error(
                    "ingestion.failed",
                    isin=isin,
                    run_id=str(run_id),
                    error=error_message.encode("ascii", "replace").decode("ascii"),
                )
            return IngestionResult(
                isin=isin,
                status="FAILED",
                run_id=run_id,
                records_seen=records_seen,
                records_written=records_written,
                message=error_message,
            )
        finally:
            if owned_client and client is not None:
                await client.__aexit__(None, None, None)


async def ingest_many(isins: list[str]) -> list[IngestionResult]:
    results: list[IngestionResult] = []
    async with OeKBClient() as client:
        for isin in isins:
            results.append(await ingest_isin(isin, client=client))
    return results


async def _log_run_started(*, session: AsyncSession, run_id: uuid.UUID, isin: str) -> IMPLOG:
    log_entry = IMPLOG(
        run_id=run_id,
        isin=isin,
        status="STARTED",
        started_at=datetime.now(timezone.utc),
        records_seen=0,
        records_written=0,
    )
    session.add(log_entry)
    await session.commit()
    await session.refresh(log_entry)
    return log_entry


async def _upsert(
    session: AsyncSession,
    model: type,
    values: dict,
    conflict_columns: list[object],
    *,
    allow_equal_version_update: bool = False,
    preserve_existing_on_null: bool = False,
) -> None:
    bind = session.get_bind()
    if bind is None:
        raise RuntimeError("Session is not bound to an engine.")

    dialect = bind.dialect.name
    insert_fn = pg_insert if dialect == "postgresql" else sqlite_insert

    statement = insert_fn(model).values(**values)
    excluded = statement.excluded
    mapped_columns = {
        attr: getattr(model, attr).property.columns[0]
        for attr in values
        if hasattr(model, attr)
    }
    set_values = {
        column.name: (
            func.coalesce(getattr(excluded, column.name), column)
            if preserve_existing_on_null
            else getattr(excluded, column.name)
        )
        for attr, column in mapped_columns.items()
        if attr not in {"id", "created_at", "updated_at"}
    }
    if hasattr(model, "updated_at"):
        updated_col = model.updated_at.property.columns[0]
        set_values[updated_col.name] = func.now()

    where_clause = None
    if "versions_nr" in values and hasattr(model, "versions_nr"):
        versions_col = model.versions_nr.property.columns[0]
        if allow_equal_version_update:
            where_clause = getattr(excluded, versions_col.name) >= versions_col
        else:
            where_clause = getattr(excluded, versions_col.name) > versions_col

    conflict_index_elements = [
        column.property.columns[0] if hasattr(column, "property") else column
        for column in conflict_columns
    ]
    statement = statement.on_conflict_do_update(
        index_elements=conflict_index_elements,
        set_=set_values,
        where=where_clause,
    )
    await session.execute(statement)


async def _upsert_security_master(
    *,
    session: AsyncSession,
    isin: str,
    reports: list[OeKBReportListItem],
) -> None:
    existing = await session.scalar(select(SECMDA).where(SECMDA.isin == isin))
    name_candidate = next((report.isin_bez for report in reports if report.isin_bez), None)
    currency_candidate = next((report.waehrung for report in reports if report.waehrung), None)

    values = {
        "isin": isin,
        "name": name_candidate or (existing.name if existing is not None else isin),
        "waehrung": currency_candidate,
        "domicile_ctr": existing.domicile_ctr if existing is not None else None,
        "ertragstyp": existing.ertragstyp if existing is not None else None,
    }
    await _upsert(
        session,
        SECMDA,
        values,
        [SECMDA.isin],
        preserve_existing_on_null=True,
    )


async def _should_persist_source_report(
    *,
    session: AsyncSession,
    isin: str,
    stm_id: int,
    incoming_version: int,
    incoming_payload: dict,
) -> tuple[bool, bool]:
    existing_raw = await session.scalar(
        select(SOURCERAW).where(SOURCERAW.isin == isin, SOURCERAW.stm_id == stm_id)
    )
    if existing_raw is None:
        return True, True

    current_version = existing_raw.versions_nr
    if incoming_version > current_version:
        return True, False
    if incoming_version < current_version:
        return False, False

    payload_changed = existing_raw.payload != incoming_payload
    if payload_changed:
        return True, True
    return False, False


async def _ensure_tax_dictionaries(*, session: AsyncSession) -> None:
    for line in LINE_DICTIONARY:
        await _upsert(
            session,
            TAXLIN,
            {
                "line_code": line["line_code"],
                "metric_key": line["metric_key"],
                "name_de": line["name_de"],
                "name_en": line["name_en"],
                "line_order": line["line_order"],
                "is_active": True,
                "valid_from": None,
                "valid_to": None,
            },
            [TAXLIN.line_code],
        )

    for category in CATEGORY_DICTIONARY:
        await _upsert(
            session,
            TAXCAT,
            {
                "category_code": category["category_code"],
                "category_key": category["category_key"],
                "name_de": category["name_de"],
                "name_en": category["name_en"],
                "category_order": category["category_order"],
            },
            [TAXCAT.category_code],
        )


async def _curate_report(*, session: AsyncSession, isin: str, stm_id: int) -> None:
    source_rpt = await session.scalar(
        select(SOURCERPT).where(SOURCERPT.isin == isin, SOURCERPT.stm_id == stm_id)
    )
    source_age = await session.scalar(
        select(SOURCEAGE).where(SOURCEAGE.isin == isin, SOURCEAGE.stm_id == stm_id)
    )
    if source_rpt is None or source_age is None:
        return

    await _upsert(
        session,
        TAXRPT,
        {
            "isin": source_rpt.isin,
            "stm_id": source_rpt.stm_id,
            "versions_nr": source_rpt.versions_nr,
            "status_code": source_rpt.status_code,
            "report_year": source_rpt.report_year,
            "meldg_datum": source_rpt.meldg_datum,
            "waehrung": source_rpt.waehrung,
            "isin_bez": source_rpt.isin_bez,
            "gueltig_von": source_rpt.gueltig_von,
            "gueltig_bis": source_rpt.gueltig_bis,
            "bus_year_begin": source_rpt.gj_beginn,
            "bus_year_end": source_rpt.gj_ende,
            "zufluss": source_rpt.zufluss,
            "jahresmeldung": source_rpt.jahresmeldung,
            "ausschuettungsmeldung": source_rpt.ausschuettungsmeldung,
            "selbstnachweis": source_rpt.selbstnachweis,
            "korrigierte_stm_id": source_rpt.korrigierte_stm_id,
        },
        [TAXRPT.isin, TAXRPT.stm_id],
        allow_equal_version_update=True,
        preserve_existing_on_null=True,
    )

    tax_rpt = await session.scalar(select(TAXRPT).where(TAXRPT.isin == isin, TAXRPT.stm_id == stm_id))
    if tax_rpt is None:
        return

    await session.execute(delete(TAXDAT).where(TAXDAT.taxrpt_id == tax_rpt.id))
    await session.execute(delete(TAXADJ).where(TAXADJ.taxrpt_id == tax_rpt.id))

    line_ids = {
        code: tax_id
        for code, tax_id in (await session.execute(select(TAXLIN.line_code, TAXLIN.id))).all()
    }
    category_ids = {
        code: tax_id
        for code, tax_id in (await session.execute(select(TAXCAT.category_code, TAXCAT.id))).all()
    }

    for metric_key, metric_code in METRIC_CODE_BY_KEY.items():
        line_id = line_ids.get(metric_code)
        if line_id is None:
            continue

        for category_key, category_code in CATEGORY_CODE_BY_KEY.items():
            category_id = category_ids.get(category_code)
            if category_id is None:
                continue

            attr_name = f"{metric_key}_{category_key}"
            value = getattr(source_age, attr_name)
            if value is None:
                continue

            await _upsert(
                session,
                TAXDAT,
                {
                    "taxrpt_id": tax_rpt.id,
                    "okb_id": tax_rpt.stm_id,
                    "taxlin_id": line_id,
                    "taxcat_id": category_id,
                    "amount": value,
                    "waehrung": tax_rpt.waehrung,
                },
                [TAXDAT.taxrpt_id, TAXDAT.taxlin_id, TAXDAT.taxcat_id],
            )

            if metric_code == "K61":
                await _upsert(
                    session,
                    TAXADJ,
                    {
                        "taxrpt_id": tax_rpt.id,
                        "okb_id": tax_rpt.stm_id,
                        "taxcat_id": category_id,
                        "adj_code": "AKC",
                        "amount": value,
                        "waehrung": tax_rpt.waehrung,
                    },
                    [TAXADJ.taxrpt_id, TAXADJ.taxcat_id, TAXADJ.adj_code],
                )

    if source_rpt.ausschuettungsmeldung and source_rpt.zufluss is not None:
        await _upsert(
            session,
            SECDIV,
            {
                "isin": source_rpt.isin,
                "okb_id": source_rpt.stm_id,
                "flow_type": "DIST",
                "flow_date": source_rpt.zufluss,
                "cash_amount": None,
                "waehrung": source_rpt.waehrung,
                "report_year": source_rpt.report_year,
                "status_code": source_rpt.status_code,
            },
            [SECDIV.isin, SECDIV.flow_date, SECDIV.flow_type, SECDIV.okb_id],
            preserve_existing_on_null=True,
        )

    if source_rpt.korrigierte_stm_id is not None:
        old_tax_rpt = await session.scalar(
            select(TAXRPT).where(TAXRPT.isin == source_rpt.isin, TAXRPT.stm_id == source_rpt.korrigierte_stm_id)
        )
        if old_tax_rpt is not None and old_tax_rpt.id != tax_rpt.id:
            await _upsert(
                session,
                TAXCOR,
                {
                    "old_taxrpt_id": old_tax_rpt.id,
                    "new_taxrpt_id": tax_rpt.id,
                    "reason_code": "KID",
                },
                [TAXCOR.old_taxrpt_id, TAXCOR.new_taxrpt_id],
            )
