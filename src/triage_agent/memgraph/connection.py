"""Memgraph connection management via Bolt protocol."""

import time
from functools import lru_cache
from typing import Any

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, TransientError

from triage_agent.config import get_config


class MemgraphConnection:
    """Memgraph connection with pooling and retry logic."""

    def __init__(
        self,
        uri: str,
        max_connection_pool_size: int = 10,
        max_retries: int = 3,
    ) -> None:
        self._driver = GraphDatabase.driver(
            uri,
            max_connection_pool_size=max_connection_pool_size,
        )
        self._max_retries = max_retries

    def execute_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        max_retries: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a read-only Cypher query with retry logic."""
        retries = max_retries if max_retries is not None else self._max_retries
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                with self._driver.session() as session:
                    result = session.run(query, params or {})
                    return [dict(record) for record in result]
            except (ServiceUnavailable, TransientError) as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff

        if last_error:
            raise last_error
        raise RuntimeError("Unexpected error in execute_cypher")

    def execute_cypher_write(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        max_retries: int | None = None,
    ) -> None:
        """Execute a write Cypher query with retry logic."""
        retries = max_retries if max_retries is not None else self._max_retries
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                with self._driver.session() as session:
                    session.run(query, params or {})
                    return
            except (ServiceUnavailable, TransientError) as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(2**attempt)
        if last_error:
            raise last_error
        raise RuntimeError("Unexpected error in execute_cypher_write")  # pragma: no cover

    def health_check(self) -> bool:
        """Check if Memgraph is accessible."""
        try:
            result = self.execute_cypher("RETURN 1 AS health")
            return len(result) == 1 and result[0].get("health") == 1
        except Exception:
            return False

    def load_reference_dag(self, dag_name: str) -> dict[str, Any] | None:
        """Load a reference DAG by name."""
        query = """
        MATCH (t:ReferenceTrace {name: $dag_name})-[:STEP]->(e:RefEvent)
        RETURN t.name AS name, t.spec AS spec, t.procedure AS procedure,
               collect({
                   order: e.order,
                   nf: e.nf,
                   action: e.action,
                   keywords: e.keywords,
                   optional: e.optional,
                   sub_dag: e.sub_dag
               }) AS phases
        """
        results = self.execute_cypher(query, {"dag_name": dag_name})
        if not results:
            return None

        result = results[0]
        phases = sorted(result.get("phases", []), key=lambda x: x.get("order", 0))

        # Extract all unique NFs from phases
        all_nfs = list({phase.get("nf") for phase in phases if phase.get("nf")})

        return {
            "name": result.get("name"),
            "spec": result.get("spec"),
            "procedure": result.get("procedure"),
            "phases": phases,
            "all_nfs": all_nfs,
        }

    def ingest_captured_trace(
        self,
        incident_id: str,
        imsi: str,
        events: list[dict[str, Any]],
    ) -> None:
        """Ingest a captured IMSI trace into Memgraph."""
        query = """
        CREATE (t:CapturedTrace {incident_id: $incident_id, imsi: $imsi})
        WITH t
        UNWIND $events AS event
        CREATE (t)-[:EVENT]->(e:TraceEvent {
            order: event.order,
            message: event.message,
            timestamp: event.timestamp,
            nf: event.nf
        })
        """
        self.execute_cypher_write(
            query,
            {
                "incident_id": incident_id,
                "imsi": imsi,
                "events": events,
            },
        )

    def ingest_captured_traces_batch(
        self,
        incident_id: str,
        traces: list[dict[str, Any]],
    ) -> None:
        """Ingest all IMSI traces for an incident in a single Bolt round-trip.

        Each element of ``traces`` must have the shape produced by
        ``contract_imsi_trace``: ``{"imsi": str, "events": list[dict]}``.
        Replaces N sequential ``ingest_captured_trace`` calls with one query.
        """
        if not traces:
            return
        query = """
        UNWIND $traces AS trace
        CREATE (t:CapturedTrace {incident_id: $incident_id, imsi: trace.imsi})
        WITH t, trace
        UNWIND trace.events AS event
        CREATE (t)-[:EVENT]->(e:TraceEvent {
            order: event.order,
            message: event.message,
            timestamp: event.timestamp,
            nf: event.nf
        })
        """
        self.execute_cypher_write(query, {"incident_id": incident_id, "traces": traces})

    def detect_deviation(
        self,
        incident_id: str,
        imsi: str,
        dag_name: str,
    ) -> dict[str, Any] | None:
        """Detect first deviation between captured trace and reference DAG."""
        query = """
        MATCH (ref:ReferenceTrace {name: $dag_name})-[:STEP]->(refStep:RefEvent)
        MATCH (trace:CapturedTrace {incident_id: $incident_id, imsi: $imsi})-[:EVENT]->(event:TraceEvent)
        WHERE refStep.order = event.order AND NOT event.message CONTAINS refStep.action
        RETURN refStep.order AS deviation_point,
               refStep.action AS expected,
               event.message AS actual,
               refStep.nf AS expected_nf,
               event.nf AS actual_nf
        ORDER BY refStep.order
        LIMIT 1
        """
        results = self.execute_cypher(
            query,
            {
                "dag_name": dag_name,
                "incident_id": incident_id,
                "imsi": imsi,
            },
        )
        return results[0] if results else None

    def detect_deviations_batch(
        self,
        incident_id: str,
        dag_name: str,
    ) -> list[dict[str, Any]]:
        """Detect first deviation per IMSI against a DAG in a single query.

        Replaces the N+1 pattern (one MATCH for all IMSIs + one detect_deviation
        per IMSI) with a single Cypher query that returns one row per deviating
        IMSI. Returns the same shape as the original list of ``detect_deviation``
        results, with an added ``imsi`` field.
        """
        query = """
        MATCH (ref:ReferenceTrace {name: $dag_name})-[:STEP]->(refStep:RefEvent)
        MATCH (trace:CapturedTrace {incident_id: $incident_id})-[:EVENT]->(event:TraceEvent)
        WHERE refStep.order = event.order
          AND NOT event.message CONTAINS refStep.action
        WITH trace.imsi AS imsi,
             refStep.order    AS deviation_point,
             refStep.action   AS expected,
             event.message    AS actual,
             refStep.nf       AS expected_nf,
             event.nf         AS actual_nf
        ORDER BY imsi, deviation_point
        WITH imsi,
             collect({
                 deviation_point: deviation_point,
                 expected: expected,
                 actual: actual,
                 expected_nf: expected_nf,
                 actual_nf: actual_nf
             })[0] AS first_dev
        RETURN imsi,
               first_dev.deviation_point AS deviation_point,
               first_dev.expected        AS expected,
               first_dev.actual          AS actual,
               first_dev.expected_nf     AS expected_nf,
               first_dev.actual_nf       AS actual_nf
        """
        return self.execute_cypher(query, {"dag_name": dag_name, "incident_id": incident_id})

    def cleanup_incident_traces(self, incident_id: str) -> None:
        """Remove all captured traces for an incident."""
        query = """
        MATCH (t:CapturedTrace {incident_id: $incident_id})-[:EVENT]->(e:TraceEvent)
        DETACH DELETE t, e
        """
        self.execute_cypher_write(query, {"incident_id": incident_id})

    def close(self) -> None:
        """Close the driver connection."""
        self._driver.close()

    def __enter__(self) -> "MemgraphConnection":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


@lru_cache(maxsize=1)
def get_memgraph() -> MemgraphConnection:
    """Get singleton Memgraph connection."""
    config = get_config()
    return MemgraphConnection(
        config.memgraph_uri,
        max_connection_pool_size=config.memgraph_pool_size,
        max_retries=config.memgraph_max_retries,
    )
