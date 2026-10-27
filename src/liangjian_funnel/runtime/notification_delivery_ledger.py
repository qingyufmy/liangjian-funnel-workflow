"""Customer notification receipts only; never opens the legacy business DB."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import uuid


class NotificationDeliveryLedger:
    def __init__(self,path):
        self.path=Path(path)
        if self.path.is_symlink() or self.path.parent.is_symlink():
            raise ValueError('NOTIFICATION_LEDGER_SCOPE_INVALID')
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with closing(self._connect()) as db,db:
            db.execute('CREATE TABLE IF NOT EXISTS customer_notification_receipts ('
                'delivery_key TEXT PRIMARY KEY,delivery_id TEXT NOT NULL,kind TEXT NOT NULL,'
                'status TEXT NOT NULL,color TEXT NOT NULL,created_at TEXT NOT NULL,body_json TEXT NOT NULL)')

    def _connect(self):return sqlite3.connect(self.path,timeout=2)

    def get_delivery_by_key(self,key):
        with closing(self._connect()) as db:
            row=db.execute('SELECT body_json FROM customer_notification_receipts WHERE delivery_key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else None

    def next_notification_color(self):
        from .lark import rotate_color
        with closing(self._connect()) as db:
            row=db.execute('SELECT color FROM customer_notification_receipts ORDER BY rowid DESC LIMIT 1').fetchone()
        return rotate_color(row[0] if row else None)

    def record_delivery(self,**values):
        row={**values,'delivery_id':str(uuid.uuid4())}
        row['created_at']=values['created_at'].isoformat()
        row['sent_at']=values['sent_at'].isoformat() if values.get('sent_at') else None
        raw=json.dumps(row,ensure_ascii=False,sort_keys=True)
        with closing(self._connect()) as db,db:
            old=db.execute('SELECT body_json FROM customer_notification_receipts WHERE delivery_key=?',(values['delivery_key'],)).fetchone()
            if old:return json.loads(old[0]),False
            db.execute('INSERT INTO customer_notification_receipts VALUES (?,?,?,?,?,?,?)',
                (values['delivery_key'],row['delivery_id'],values['kind'],values['status'],values['color'],row['created_at'],raw))
        return row,True
