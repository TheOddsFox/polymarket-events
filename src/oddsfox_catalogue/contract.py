"""Explicit public projections for the replacement catalogue contract."""

CONTRACT = "oddsfox.polymarket.catalogue.v2"
NORMALIZATION_REVISION = "2"

PROJECTIONS = {
    "events": (
        "marts.mart_event_catalogue",
        "venue event_id title slug ticker active closed archived neg_risk is_open "
        "start_date end_date volume liquidity tag_labels series_titles market_count "
        "open_market_count last_observed_at observation_count",
        "venue, event_id",
    ),
    "markets": (
        "core.markets_current",
        "venue market_id observation_id source_kind condition_id question slug description "
        "source_market_version protocol active closed archived resolved neg_risk enable_order_book "
        "accepting_orders tick_size minimum_order_size created_at start_at end_at close_at "
        "resolved_at source_updated_at volume liquidity usable identity_error "
        "first_observed_at last_observed_at observation_count",
        "venue, market_id",
    ),
    "outcomes": (
        "core.outcomes_current",
        "venue market_id condition_id outcome_index outcome_label asset_kind asset_id "
        "clob_token_id position_id chain_index_set observation_id",
        "venue, market_id, outcome_index",
    ),
    "event_tags": (
        "core.event_tags_current",
        "venue event_id tag_id tag_label tag_slug",
        "venue, event_id, tag_id",
    ),
    "event_series": (
        "core.event_series_current",
        "venue event_id series_id series_slug series_title",
        "venue, event_id, series_id",
    ),
    "market_event_bridge": (
        "core.market_event_bridge",
        "venue market_id event_id membership_observation_id membership_source_kind "
        "membership_received_at inferred",
        "venue, market_id, event_id",
    ),
}


def projection_queries():
    queries = {
        name: f"SELECT {', '.join(columns.split())} FROM {relation} ORDER BY {order}"
        for name, (relation, columns, order) in PROJECTIONS.items()
    }
    queries["quarantine"] = """
SELECT venue, 'market' AS entity, market_id AS entity_id, observation_id,
       NULL::VARCHAR AS page_id, identity_error AS reason,
       NULL::TIMESTAMPTZ AS received_at
FROM core.quarantine_market_outcomes
UNION ALL
SELECT 'polymarket', q.entity, json_extract_string(q.payload, '$.id'), NULL::VARCHAR,
       q.page_id, q.reason, q.observed_at
FROM bronze.quarantined_records q JOIN bronze.batch_registry b USING(batch_id)
WHERE b.status = 'loaded'
"""
    return queries


def projection_schemas():
    """Fixed field names and DuckDB types for catalogue.v2 public relations."""
    booleans = {
        "active",
        "closed",
        "archived",
        "neg_risk",
        "is_open",
        "resolved",
        "enable_order_book",
        "accepting_orders",
        "usable",
        "inferred",
    }
    timestamps = {
        "start_date",
        "end_date",
        "last_observed_at",
        "created_at",
        "start_at",
        "end_at",
        "close_at",
        "resolved_at",
        "source_updated_at",
        "first_observed_at",
        "membership_received_at",
        "received_at",
    }
    counts = {"market_count", "open_market_count", "observation_count"}
    lists = {"tag_labels", "series_titles"}
    names = {name: columns.split() for name, (_, columns, _) in PROJECTIONS.items()}
    names["quarantine"] = [
        "venue",
        "entity",
        "entity_id",
        "observation_id",
        "page_id",
        "reason",
        "received_at",
    ]
    return {
        name: [
            {
                "name": column,
                "type": (
                    "BOOLEAN"
                    if column in booleans
                    else "TIMESTAMP WITH TIME ZONE"
                    if column in timestamps
                    else "BIGINT"
                    if column in counts
                    else "VARCHAR[]"
                    if column in lists
                    else "INTEGER"
                    if column in {"outcome_index", "chain_index_set"}
                    else "VARCHAR"
                ),
            }
            for column in columns
        ]
        for name, columns in names.items()
    }
