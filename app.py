"""Streamlit support-ticket app for Databricks Apps, backed by Lakebase
Autoscaling (managed Postgres 17).

Single file, no ORM. Postgres is the only source of truth: nothing here is
cached, and every write commits then triggers a rerun so the UI reflects
committed state. A browser refresh re-reads everything from the database.
"""

import os
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient

STATUSES = ["open", "in_progress", "resolved"]
PRIORITIES = ["low", "medium", "high"]

# Status badge palette: (background, text). Amber / blue / green.
STATUS_COLORS = {
    "open": ("#fef3c7", "#92400e"),
    "in_progress": ("#dbeafe", "#1e40af"),
    "resolved": ("#dcfce7", "#166534"),
}

# Subtle priority dot colors: low is muted, high stands out.
PRIORITY_COLORS = {
    "low": "#94a3b8",
    "medium": "#f59e0b",
    "high": "#ef4444",
}


def status_badge(status: str) -> str:
    """Return an inline HTML pill for a ticket status."""
    bg, fg = STATUS_COLORS.get(status, ("#e5e7eb", "#374151"))
    label = status.replace("_", " ").title()
    return (
        f"<span style='background:{bg};color:{fg};padding:2px 10px;"
        f"border-radius:9999px;font-size:0.75rem;font-weight:600;"
        f"white-space:nowrap;'>{label}</span>"
    )


def priority_marker(priority: str) -> str:
    """Return an inline HTML dot + label indicating priority."""
    color = PRIORITY_COLORS.get(priority, "#94a3b8")
    return (
        f"<span style='color:{color};font-size:0.9rem;'>&#9679;</span> "
        f"<span style='font-size:0.8rem;color:#475569;'>{priority.title()}</span>"
    )


def relative_time(dt) -> str:
    """Human-friendly 'time ago' for a tz-aware timestamp."""
    if dt is None:
        return ""
    seconds = (datetime.now(timezone.utc) - dt).total_seconds()
    if seconds < 60:
        return "just now"
    for unit, size in (("m", 60), ("h", 3600), ("d", 86400)):
        value = seconds / size
        if value < (60 if unit == "m" else 24 if unit == "h" else 30):
            return f"{int(value)}{unit} ago"
    return f"{dt:%Y-%m-%d}"


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
    # The Lakebase Autoscaling credential API lives under w.postgres. If it's
    # missing, the installed databricks-sdk is too old (see requirements.txt).
    if not hasattr(w, "postgres"):
        raise RuntimeError(
            "The installed databricks-sdk has no `postgres` API. Pin "
            "databricks-sdk>=0.125.0 in requirements.txt and redeploy."
        )
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


def delete_ticket(ticket_id):
    # Messages are removed automatically via ON DELETE CASCADE on the FK.
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM tickets WHERE ticket_id = %s",
            (ticket_id,),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Support Tickets", layout="wide")
st.title("🎫 Support Tickets")

# Session state holds only UI state: the selected ticket and a pending-delete
# flag. No application data is cached here — Postgres stays the source of truth.
if "selected_ticket_id" not in st.session_state:
    st.session_state.selected_ticket_id = None
if "pending_delete_id" not in st.session_state:
    st.session_state.pending_delete_id = None


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
        with st.container(border=True):
            st.markdown(
                f"**#{tid} · {title}**&nbsp;&nbsp;{status_badge(status)}",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"{priority_marker(priority)}"
                f"<span style='color:#94a3b8;'> &nbsp;·&nbsp; </span>"
                f"<span style='font-size:0.8rem;color:#475569;'>{created_by} · "
                f"{relative_time(created_at)}</span>",
                unsafe_allow_html=True,
            )
            if st.button("View / manage", key=f"select_{tid}"):
                st.session_state.selected_ticket_id = tid
                st.session_state.pending_delete_id = None
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
        st.session_state.pending_delete_id = None
        return

    tid, title, status, priority, created_by, created_at = ticket

    header = st.columns([6, 1])
    header[0].subheader(f"#{tid} · {title}")
    if header[1].button("Close"):
        st.session_state.selected_ticket_id = None
        st.session_state.pending_delete_id = None
        st.rerun()

    st.markdown(
        f"{status_badge(status)}&nbsp;&nbsp;{priority_marker(priority)}"
        f"<span style='color:#94a3b8;'> &nbsp;·&nbsp; </span>"
        f"<span style='font-size:0.8rem;color:#475569;'>Opened by {created_by} · "
        f"{relative_time(created_at)}</span>",
        unsafe_allow_html=True,
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
            st.markdown(
                f"**{author}**"
                f"<span style='color:#94a3b8;font-size:0.8rem;'> · "
                f"{relative_time(msg_created_at)}</span>",
                unsafe_allow_html=True,
            )
            st.write(message_text)

    st.divider()

    # Add a message
    with st.form(f"add_message_form_{tid}", clear_on_submit=True):
        message_text = st.text_area("New message")
        author = st.text_input("Author")
        submitted = st.form_submit_button("Add message")

    if submitted:
        # Validate without early-returning, so the delete section below still
        # renders on the same run.
        if not message_text.strip():
            st.error("Message text is required.")
        elif not author.strip():
            st.error("Author is required.")
        else:
            try:
                add_message(tid, message_text.strip(), author.strip())
            except Exception as exc:  # noqa: BLE001
                st.error(f"Could not add message: {exc}")
            else:
                st.rerun()

    st.divider()

    # Delete with confirmation — never on a single click. The first click only
    # arms a pending-delete flag; the actual DELETE runs on Confirm.
    if st.session_state.pending_delete_id == tid:
        st.warning(
            f"Delete ticket **#{tid} · {title}** and its "
            f"{len(messages)} message(s)? This cannot be undone."
        )
        confirm_col, cancel_col = st.columns(2)
        if confirm_col.button(
            "Confirm delete", key=f"confirm_delete_{tid}", type="primary"
        ):
            try:
                delete_ticket(tid)  # messages cascade via the FK
            except Exception as exc:  # noqa: BLE001
                st.error(f"Could not delete ticket: {exc}")
            else:
                st.session_state.pending_delete_id = None
                st.session_state.selected_ticket_id = None
                st.rerun()
        if cancel_col.button("Cancel", key=f"cancel_delete_{tid}"):
            st.session_state.pending_delete_id = None
            st.rerun()
    else:
        if st.button("Delete ticket", key=f"delete_{tid}"):
            st.session_state.pending_delete_id = tid
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
