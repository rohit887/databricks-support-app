"""Streamlit support-ticket app for Databricks Apps, backed by Lakebase
Autoscaling (managed Postgres 17).

Single file, no ORM. Postgres is the only source of truth: nothing here is
cached, and every write commits then triggers a rerun so the UI reflects
committed state. A browser refresh re-reads everything from the database.
"""

import os
from contextlib import contextmanager

import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient

STATUSES = ["open", "in_progress", "resolved"]
PRIORITIES = ["low", "medium", "high"]


# ---------------------------------------------------------------------------
# Connection layer
#
# Databricks Apps injects PGHOST/PGDATABASE/PGUSER/PGPORT into the environment
# at deploy time when a Lakebase database resource is bound to the app. The
# password is NOT injected — instead we mint a short-lived OAuth credential per
# connection via the Databricks SDK. These tokens expire after ~1 hour, so we
# generate a fresh one for every connection and never cache it at module scope
# or in a global.
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    """Return an env var or raise a clear, named error (never a KeyError)."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name} is not set. "
            "Databricks Apps injects the PG* variables only after the first "
            "deploy with a Lakebase resource bound, and ENDPOINT_NAME must be "
            "set in app.yaml."
        )
    return value


def _generate_password() -> str:
    """Mint a fresh, short-lived OAuth database credential.

    WorkspaceClient() takes no arguments: it reads auth from the environment
    (the app's service principal when deployed, or your user account locally).
    The endpoint resource name identifies which Lakebase compute to issue the
    credential for.
    """
    endpoint_name = _require_env("ENDPOINT_NAME")
    w = WorkspaceClient()
    credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
    return credential.token


@contextmanager
def get_connection():
    """Yield a psycopg connection built from injected PG* env vars plus a
    freshly generated OAuth token. TLS is mandatory on Lakebase.

    Used as a context manager so the connection is always closed; callers
    open a cursor (also a context manager) and commit writes explicitly.
    """
    host = _require_env("PGHOST")
    dbname = _require_env("PGDATABASE")
    user = _require_env("PGUSER")
    port = os.environ.get("PGPORT", "5432")  # default per requirements
    password = _generate_password()

    conn = psycopg.connect(
        host=host,
        dbname=dbname,
        user=user,
        port=port,
        password=password,
        sslmode="require",  # Lakebase mandates TLS
    )
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Data access — every query parameterized with %s, no string building in SQL.
# ---------------------------------------------------------------------------


def fetch_tickets(status_filter=None):
    sql = (
        "SELECT ticket_id, title, status, priority, created_by, created_at "
        "FROM tickets"
    )
    params = []
    if status_filter:
        sql += " WHERE status = %s"
        params.append(status_filter)
    sql += " ORDER BY created_at DESC, ticket_id DESC"

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_ticket(ticket_id):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ticket_id, title, status, priority, created_by, created_at "
            "FROM tickets WHERE ticket_id = %s",
            (ticket_id,),
        )
        return cur.fetchone()


def fetch_messages(ticket_id):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT message_text, author, created_at "
            "FROM ticket_messages WHERE ticket_id = %s "
            "ORDER BY created_at ASC, message_id ASC",
            (ticket_id,),
        )
        return cur.fetchall()


def fetch_status_counts():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, COUNT(*) FROM tickets GROUP BY status"
        )
        return dict(cur.fetchall())


def create_ticket(title, created_by, priority):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tickets (title, created_by, priority) "
            "VALUES (%s, %s, %s) RETURNING ticket_id",
            (title, created_by, priority),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        return new_id


def add_message(ticket_id, message_text, author):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ticket_messages (ticket_id, message_text, author) "
            "VALUES (%s, %s, %s)",
            (ticket_id, message_text, author),
        )
        conn.commit()


def update_status(ticket_id, status):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE tickets SET status = %s WHERE ticket_id = %s",
            (status, ticket_id),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Support Tickets", layout="wide")
st.title("🎫 Support Tickets")

# The only thing we keep in session state is the currently-selected ticket ID.
if "selected_ticket_id" not in st.session_state:
    st.session_state.selected_ticket_id = None


def render_stats():
    st.subheader("Stats")
    try:
        counts = fetch_status_counts()
    except Exception as exc:  # noqa: BLE001 — surface a readable message
        st.error(f"Could not load stats: {exc}")
        return
    cols = st.columns(len(STATUSES) + 1)
    total = 0
    for col, status in zip(cols, STATUSES):
        n = counts.get(status, 0)
        total += n
        col.metric(status.replace("_", " ").title(), n)
    cols[-1].metric("Total", total)


def render_ticket_list():
    st.subheader("Tickets")

    status_filter = st.selectbox(
        "Filter by status",
        options=["all"] + STATUSES,
        format_func=lambda s: "All" if s == "all" else s.replace("_", " ").title(),
    )
    effective_filter = None if status_filter == "all" else status_filter

    try:
        tickets = fetch_tickets(effective_filter)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load tickets: {exc}")
        return

    if not tickets:
        st.info("No tickets match this filter.")
        return

    for tid, title, status, priority, created_by, created_at in tickets:
        label = f"#{tid} · {title}"
        with st.container(border=True):
            st.markdown(f"**{label}**")
            st.caption(
                f"Status: `{status}`  |  Priority: `{priority}`  |  "
                f"By: {created_by}  |  {created_at:%Y-%m-%d %H:%M}"
            )
            if st.button("View / manage", key=f"select_{tid}"):
                st.session_state.selected_ticket_id = tid
                st.rerun()


def render_create_ticket():
    st.subheader("Create a ticket")
    with st.form("create_ticket_form", clear_on_submit=True):
        title = st.text_input("Title")
        created_by = st.text_input("Created by")
        priority = st.selectbox("Priority", options=PRIORITIES, index=1)
        submitted = st.form_submit_button("Create ticket")

    if submitted:
        # Validate before touching the database.
        if not title.strip():
            st.error("Title is required.")
            return
        if not created_by.strip():
            st.error("Created by is required.")
            return
        try:
            new_id = create_ticket(title.strip(), created_by.strip(), priority)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not create ticket: {exc}")
            return
        st.session_state.selected_ticket_id = new_id
        st.rerun()


def render_ticket_detail(ticket_id):
    try:
        ticket = fetch_ticket(ticket_id)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load ticket: {exc}")
        return

    if ticket is None:
        st.warning("That ticket no longer exists.")
        st.session_state.selected_ticket_id = None
        return

    tid, title, status, priority, created_by, created_at = ticket

    header = st.columns([6, 1])
    header[0].subheader(f"#{tid} · {title}")
    if header[1].button("Close"):
        st.session_state.selected_ticket_id = None
        st.rerun()

    st.caption(
        f"Priority: `{priority}`  |  By: {created_by}  |  "
        f"{created_at:%Y-%m-%d %H:%M}"
    )

    # Update status
    new_status = st.selectbox(
        "Status",
        options=STATUSES,
        index=STATUSES.index(status),
        key=f"status_{tid}",
    )
    if new_status != status:
        if st.button("Save status", key=f"save_status_{tid}"):
            try:
                update_status(tid, new_status)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Could not update status: {exc}")
                return
            st.rerun()

    st.divider()

    # Messages, chronological
    st.markdown("### Messages")
    try:
        messages = fetch_messages(tid)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load messages: {exc}")
        return

    if not messages:
        st.info("No messages yet.")
    for message_text, author, msg_created_at in messages:
        with st.chat_message("user"):
            st.markdown(f"**{author}** · {msg_created_at:%Y-%m-%d %H:%M}")
            st.write(message_text)

    # Add a message
    with st.form(f"add_message_form_{tid}", clear_on_submit=True):
        message_text = st.text_area("New message")
        author = st.text_input("Author")
        submitted = st.form_submit_button("Add message")

    if submitted:
        if not message_text.strip():
            st.error("Message text is required.")
            return
        if not author.strip():
            st.error("Author is required.")
            return
        try:
            add_message(tid, message_text.strip(), author.strip())
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not add message: {exc}")
            return
        st.rerun()


# Layout: list + create on the left, detail on the right.
render_stats()
st.divider()

left, right = st.columns(2)
with left:
    render_ticket_list()
    st.divider()
    render_create_ticket()
with right:
    if st.session_state.selected_ticket_id is not None:
        render_ticket_detail(st.session_state.selected_ticket_id)
    else:
        st.info("Select a ticket to view its messages and manage it.")
