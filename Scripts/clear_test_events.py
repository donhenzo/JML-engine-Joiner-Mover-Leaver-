import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from pathlib import Path

settings = json.loads(Path("local.settings.json").read_text())
conn = settings["Values"]["JML_STORAGE_CONNECTION_STRING"]

from Functions.Event_store.event_store import get_events_table_client, generate_event_id

client = get_events_table_client(conn)

records = [
    ("E401", "Joiner", "2026-06-01"),
    ("E402", "Joiner", "2026-06-01"),
    ("E403", "Joiner", "2026-06-01"),
    ("E404", "Joiner", "2026-06-01"),
    ("E405", "Joiner", "2026-06-01"),
    ("E406", "Joiner", "2026-06-01"),
    ("E407", "Joiner", "2026-06-01"),
    ("E408", "Joiner", "2026-06-01"),
    ("E410", "Joiner", "2026-06-01"),
]

for employee_id, action, start_date in records:
    event_id = generate_event_id(employee_id, action, start_date)
    try:
        client.delete_entity(partition_key=employee_id, row_key=event_id)
        print(f"Deleted — {employee_id}")
    except Exception as e:
        print(f"Failed — {employee_id}: {e}")