"""Routes for browsing insertion order files from the K: drive."""

import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import (
    Blueprint,
    abort,
    render_template_string,
    send_from_directory,
)

logger = logging.getLogger(__name__)

insertion_orders_bp = Blueprint(
    "insertion_orders", __name__, url_prefix="/reports/insertion-orders"
)

IO_BASE_PATH = "/mnt/k-drive/Insertion Orders"

LISTING_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<title>{{ title }}</title>
<style>
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    max-width: 800px; margin: 40px auto; padding: 0 20px;
    background: #2e3440; color: #d8dee9;
  }
  a { color: #88c0d0; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .breadcrumb { font-size: 14px; color: #81a1c1; margin-bottom: 16px; }
  h1 { font-size: 22px; font-weight: 600; color: #eceff4; margin-bottom: 20px; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 12px; text-transform: uppercase;
       color: #81a1c1; border-bottom: 1px solid #4c566a; padding: 8px 12px; }
  td { padding: 10px 12px; border-bottom: 1px solid #3b4252; }
  tr:hover { background: #3b4252; }
  .size { color: #81a1c1; font-size: 13px; white-space: nowrap; }
  .date { color: #81a1c1; font-size: 13px; white-space: nowrap; }
  .icon { margin-right: 6px; }
  .empty { color: #81a1c1; font-style: italic; padding: 20px 0; }
</style>
</head>
<body>
  <div class="breadcrumb">{{ breadcrumb | safe }}</div>
  <h1>{{ title }}</h1>
  {% if items %}
  <table>
    <thead><tr>
      <th>Name</th>
      {% if show_size %}<th>Size</th><th>Modified</th>{% endif %}
    </tr></thead>
    <tbody>
    {% for item in items %}
      <tr>
        <td><span class="icon">{{ item.icon }}</span>
            <a href="{{ item.href }}">{{ item.name }}</a></td>
        {% if show_size %}
        <td class="size">{{ item.size }}</td>
        <td class="date">{{ item.modified }}</td>
        {% endif %}
      </tr>
    {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p class="empty">No items found.</p>
  {% endif %}
</body>
</html>
"""


def _safe_segment(segment):
    """Reject path traversal attempts."""
    if ".." in segment or "/" in segment or "\\" in segment:
        abort(400)
    return segment


def _format_size(size_bytes):
    """Format file size for display."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def _format_mtime(mtime):
    """Format modification time in Pacific."""
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    pacific = dt.astimezone(ZoneInfo("America/Los_Angeles"))
    return pacific.strftime("%b %d, %Y %I:%M %p")


@insertion_orders_bp.route("/<ae_name>")
def list_customers(ae_name):
    """List customer folders for an AE."""
    ae_name = _safe_segment(ae_name)
    ae_path = os.path.join(IO_BASE_PATH, ae_name)

    if not os.path.isdir(ae_path):
        abort(404)

    folders = sorted(
        entry
        for entry in os.listdir(ae_path)
        if os.path.isdir(os.path.join(ae_path, entry))
    )

    items = [
        {
            "name": folder,
            "href": f"/reports/insertion-orders/{ae_name}/{folder}",
            "icon": "📁",
        }
        for folder in folders
    ]

    breadcrumb = (
        '<a href="/reports/ae-dashboard-personal">Dashboard</a>'
        f" / {ae_name}"
    )

    return render_template_string(
        LISTING_TEMPLATE,
        title=f"Insertion Orders — {ae_name}",
        breadcrumb=breadcrumb,
        items=items,
        show_size=False,
    )


@insertion_orders_bp.route("/<ae_name>/<customer_folder>")
def list_files(ae_name, customer_folder):
    """List files in a customer's insertion order folder."""
    ae_name = _safe_segment(ae_name)
    customer_folder = _safe_segment(customer_folder)
    folder_path = os.path.join(IO_BASE_PATH, ae_name, customer_folder)

    if not os.path.isdir(folder_path):
        abort(404)

    entries = sorted(os.listdir(folder_path))
    items = []
    for entry in entries:
        full = os.path.join(folder_path, entry)
        if not os.path.isfile(full):
            continue
        stat = os.stat(full)
        items.append({
            "name": entry,
            "href": (
                f"/reports/insertion-orders"
                f"/{ae_name}/{customer_folder}/{entry}"
            ),
            "icon": "📄",
            "size": _format_size(stat.st_size),
            "modified": _format_mtime(stat.st_mtime),
        })

    breadcrumb = (
        '<a href="/reports/ae-dashboard-personal">Dashboard</a>'
        f' / <a href="/reports/insertion-orders/{ae_name}">{ae_name}</a>'
        f" / {customer_folder}"
    )

    return render_template_string(
        LISTING_TEMPLATE,
        title=customer_folder,
        breadcrumb=breadcrumb,
        items=items,
        show_size=True,
    )


@insertion_orders_bp.route("/<ae_name>/<customer_folder>/<filename>")
def serve_file(ae_name, customer_folder, filename):
    """Serve an insertion order file. PDFs inline, others as download."""
    ae_name = _safe_segment(ae_name)
    customer_folder = _safe_segment(customer_folder)
    filename = _safe_segment(filename)
    folder_path = os.path.join(IO_BASE_PATH, ae_name, customer_folder)

    if not os.path.isdir(folder_path):
        abort(404)

    file_path = os.path.join(folder_path, filename)
    if not os.path.isfile(file_path):
        abort(404)

    is_pdf = filename.lower().endswith(".pdf")
    return send_from_directory(
        folder_path,
        filename,
        as_attachment=not is_pdf,
    )
