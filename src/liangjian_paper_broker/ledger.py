"""Independent SQLite paper ledger. All bar fills and cash/lot updates are atomic.

No production database, model, network or live account dependency. Monetary
values are canonical Decimal strings; SQL never computes money using REAL.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Callable

from .matching import MinuteBar, PaperOrder, PaperRules, local_time, match_order, money, number, total_fees

ZERO = Decimal("0")


def dump(value) -> str:
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,
                      default=lambda x: x.isoformat() if isinstance(x,datetime) else str(x))


def digest(value) -> str:
    return hashlib.sha256(dump(value).encode()).hexdigest()


class LocalPaperBroker:
    def __init__(self, database: str | Path, *, initial_capital: Decimal,
                 clock: Callable[[], datetime], run_id: str = "local-paper", policy:PaperRules|None=None):
        self.path = Path(database).resolve(); self.clock = clock; self.run_id = run_id
        self.policy = {k:v for k,v in asdict(policy or PaperRules(local_time(clock()).date().isoformat())).items()
                       if k not in ("trade_date","trading_day")}
        initial_capital = number(initial_capital)
        if initial_capital <= 0 or not run_id: raise ValueError("PAPER_ACCOUNT_INVALID")
        if self.path.exists():
            with sqlite3.connect(self.path.as_uri()+"?mode=ro",uri=True,timeout=2) as c:
                tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables and "paper_meta" not in tables: raise ValueError("PAPER_DATABASE_SCOPE_INVALID")
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with self._tx() as c:
            c.execute("CREATE TABLE IF NOT EXISTS paper_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            existing = c.execute("SELECT value FROM paper_meta WHERE key='initial_capital'").fetchone()
            if existing and Decimal(existing[0]) != initial_capital: raise ValueError("PAPER_INITIAL_CAPITAL_CONFLICT")
            old_policy=c.execute("SELECT value FROM paper_meta WHERE key='fee_policy'").fetchone()
            if old_policy and old_policy[0]!=dump(self.policy): raise ValueError("PAPER_RULE_POLICY_CONFLICT")
            c.execute("INSERT OR IGNORE INTO paper_meta VALUES ('fee_policy',?)",(dump(self.policy),))
            for key,value in (("initial_capital",str(initial_capital)),("cash",str(initial_capital)),("realized","0"),("risk_peak_nav",str(initial_capital)),("risk_halt_reason","")):
                c.execute("INSERT OR IGNORE INTO paper_meta VALUES (?,?)",(key,value))
            c.execute("CREATE TABLE IF NOT EXISTS paper_orders(order_id TEXT PRIMARY KEY,client_order_id TEXT UNIQUE NOT NULL,request_hash TEXT NOT NULL,trade_date TEXT NOT NULL,symbol TEXT NOT NULL,plan_id TEXT NOT NULL,status TEXT NOT NULL,body_json TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS paper_fills(fill_id TEXT PRIMARY KEY,order_id TEXT NOT NULL,symbol TEXT NOT NULL,plan_id TEXT NOT NULL,side TEXT NOT NULL,qty INTEGER NOT NULL,price TEXT NOT NULL,fee TEXT NOT NULL,minute_end TEXT NOT NULL,run_id TEXT NOT NULL,source_signal_id TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS paper_lots(lot_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,plan_id TEXT NOT NULL,open_date TEXT NOT NULL,qty INTEGER NOT NULL,cost TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS paper_bars(symbol TEXT NOT NULL,minute_end TEXT NOT NULL,sha256 TEXT NOT NULL,observed_at TEXT NOT NULL,PRIMARY KEY(symbol,minute_end))")
            c.execute("CREATE TABLE IF NOT EXISTS paper_marks(symbol TEXT PRIMARY KEY,price TEXT NOT NULL,trade_date TEXT NOT NULL,minute_end TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS exec_ledger(event_id TEXT PRIMARY KEY,kind TEXT NOT NULL,payload_json TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS daily_nav(trade_date TEXT PRIMARY KEY,payload_json TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS paper_admissions(signal_id TEXT PRIMARY KEY,binding_hash TEXT NOT NULL,trade_date TEXT NOT NULL,result_json TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS paper_risk_days(trade_date TEXT PRIMARY KEY,body_json TEXT NOT NULL,halt_reasons_json TEXT NOT NULL)")

    @contextmanager
    def _tx(self, *, readonly=False):
        c = sqlite3.connect(self.path.as_uri()+"?mode=ro" if readonly else str(self.path),uri=readonly,timeout=2,isolation_level=None)
        c.row_factory = sqlite3.Row
        try:
            if readonly: c.execute("PRAGMA query_only=ON")
            c.execute("BEGIN" if readonly else "BEGIN IMMEDIATE")
            yield c
            c.rollback() if readonly else c.commit()
        except BaseException:
            c.rollback(); raise
        finally: c.close()

    @staticmethod
    def _meta(c,key):
        return Decimal(c.execute("SELECT value FROM paper_meta WHERE key=?",(key,)).fetchone()[0])

    @staticmethod
    def _set(c,key,value):
        c.execute("UPDATE paper_meta SET value=? WHERE key=?",(str(value),key))

    @staticmethod
    def _order(row) -> PaperOrder:
        body = json.loads(row["body_json"]); body["submitted_at"] = datetime.fromisoformat(body["submitted_at"])
        return PaperOrder(**body)

    def _open(self,c):
        return c.execute("SELECT * FROM paper_orders WHERE status IN ('OPEN','PARTIAL') ORDER BY json_extract(body_json,'$.submitted_at'),rowid").fetchall()

    def _reserved(self,c,exclude=None):
        result = ZERO
        for row in self._open(c):
            if row["order_id"] == exclude: continue
            o = self._order(row)
            if o.side != "BUY": continue
            price = min(o.limit_price,o.no_chase_price) if o.order_type == "LIMIT" else o.no_chase_price
            rules = PaperRules(row["trade_date"],**self.policy)
            result += price*o.remaining_qty + max(ZERO,total_fees(o.filled_notional+price*o.remaining_qty,o,rules)-o.fees_paid)
        return result

    @staticmethod
    def _sellable(c,symbol,plan_id,day):
        return sum(r[0] for r in c.execute("SELECT qty FROM paper_lots WHERE symbol=? AND plan_id=? AND open_date<? AND qty>0",(symbol,plan_id,day)))

    def place_order(self,symbol,side,qty,*,order_type,limit_price,client_order_id,plan_id,signal_id,
                    no_chase_price=None,tradable=False) -> str:
        with self._tx() as c:
            return self._place_order_tx(c,symbol,side,qty,order_type=order_type,limit_price=limit_price,
                client_order_id=client_order_id,plan_id=plan_id,signal_id=signal_id,
                no_chase_price=no_chase_price,tradable=tradable,now=local_time(self.clock()))

    def _place_order_tx(self,c,symbol,side,qty,*,order_type,limit_price,client_order_id,plan_id,signal_id,
                        no_chase_price=None,tradable=False,now) -> str:
        day = now.date().isoformat()
        if tradable is not True: raise ValueError("PAPER_NOT_TRADABLE")
        if not (time(9,30)<=now.time().replace(tzinfo=None)<time(11,30) or time(13)<=now.time().replace(tzinfo=None)<time(15)):
            raise ValueError("PAPER_ORDER_OUTSIDE_SESSION")
        request = dict(symbol=symbol,side=side,qty=qty,order_type=order_type,limit_price=limit_price,
                       client_order_id=client_order_id,plan_id=plan_id,signal_id=signal_id,no_chase_price=no_chase_price)
        request_hash = digest(request); oid = "paper-"+digest(client_order_id)[:24]
        old = c.execute("SELECT * FROM paper_orders WHERE client_order_id=?",(client_order_id,)).fetchone()
        if old:
            if old["request_hash"] != request_hash: raise ValueError("PAPER_IDEMPOTENCY_CONFLICT")
            return old["order_id"]
        if c.execute("SELECT 1 FROM daily_nav WHERE trade_date=?",(day,)).fetchone():raise ValueError("PAPER_DAY_SETTLED")
        o = PaperOrder(oid,plan_id,signal_id,symbol,side,order_type,qty,qty,now,
                       limit_price=limit_price,no_chase_price=no_chase_price)
        if side == "BUY" and (qty % 100 or o.no_chase_price is None or o.no_chase_price <= 0):
            raise ValueError("PAPER_BUY_ROUND_LOT_AND_PRICE_CAP_REQUIRED")
        if side == "SELL":
            reserved = sum(self._order(r).remaining_qty for r in self._open(c) if r["symbol"]==symbol and r["plan_id"]==plan_id and self._order(r).side=="SELL")
            available = self._sellable(c,symbol,plan_id,day)-reserved
            if qty>available: raise ValueError("PAPER_T1_NOT_SELLABLE")
            o = replace(o,sellable_qty=qty)
        c.execute("INSERT INTO paper_orders VALUES (?,?,?,?,?,?,?,?)",(oid,client_order_id,request_hash,day,symbol,plan_id,"OPEN",dump(asdict(o))))
        if self._reserved(c)>self._meta(c,"cash"): raise ValueError("PAPER_INSUFFICIENT_CASH")
        self._event(c,"order:"+oid,"ORDER_ACCEPTED",dict(request,order_id=oid,submitted_at=now,run_id=self.run_id))
        return oid

    @staticmethod
    def _admissions(c,day):
        return tuple(json.loads(row[0]) for row in c.execute(
            "SELECT result_json FROM paper_admissions WHERE trade_date=? ORDER BY signal_id", (day,)))

    def get_admissions(self,trade_date):
        with self._tx(readonly=True) as c:
            return self._admissions(c,trade_date)

    @staticmethod
    def _risk_day(c,day):
        row=c.execute("SELECT body_json,halt_reasons_json FROM paper_risk_days WHERE trade_date=?",(day,)).fetchone()
        return {**json.loads(row[0]),"halt_reason_codes":json.loads(row[1])} if row else None

    def open_risk_day(self,trade_date):
        """Seal an account-owned premarket baseline once, never reset it intraday."""
        with self._tx() as c:
            now=local_time(self.clock())
            if now.date().isoformat()!=trade_date:raise ValueError("PAPER_RISK_DAY_DATE_INVALID")
            old=self._risk_day(c,trade_date)
            if old is not None:return old
            if now.time().replace(tzinfo=None)>=time(9,30):raise ValueError("PAPER_RISK_DAY_OPEN_TIME_INVALID")
            if c.execute("SELECT 1 FROM paper_orders WHERE trade_date=?",(trade_date,)).fetchone():
                raise ValueError("PAPER_RISK_DAY_ALREADY_TRADED")
            account=self._account(c,trade_date);positions=self._positions(c,trade_date)
            if account['nav'] is None:raise ValueError("PAPER_RISK_DAY_NAV_UNPROVEN")
            if positions:
                prior=c.execute("SELECT payload_json FROM daily_nav WHERE trade_date<? ORDER BY trade_date DESC LIMIT 1",(trade_date,)).fetchone()
                report=json.loads(prior[0]) if prior else {}
                if (report.get('status')!='COMPLETE' or report.get('reconcile',{}).get('status')!='MATCH'
                        or report.get('account',{}).get('nav')!=account['nav']):
                    raise ValueError("PAPER_RISK_DAY_PRIOR_CLOSE_UNPROVEN")
            basis=dict(trade_date=trade_date,opened_at=now.isoformat(),day_open_nav=account['nav'],
                source_sha256=digest({'account':account,'positions':positions}))
            c.execute("INSERT INTO paper_risk_days VALUES(?,?,?)",(trade_date,dump(basis),'[]'))
            self._event(c,'risk-open:'+trade_date,'RISK_DAY_OPEN',basis)
            return {**basis,'halt_reason_codes':[]}

    def admit_signal(self,signal_id,*,binding,decide,mode="control"):
        """Trusted pure risk callback under the same lock as paper reservation.

        The callback gets fresh positions, open orders and committed daily
        admissions. It must not perform network/model calls or open this DB.
        No strategy limit lives here; the project risk engine remains owner.
        A record-mode judgment is auditable but never consumes a trading slot.
        """
        if mode not in {"control","record"} or not isinstance(signal_id,str) or not signal_id.strip():
            raise ValueError("PAPER_ADMISSION_ID_INVALID")
        if (not isinstance(binding,dict) or any(not isinstance(binding.get(k),str) or not binding[k]
                for k in ("plan_id","symbol","trade_date"))):
            raise ValueError("PAPER_ADMISSION_BINDING_INVALID")
        # Canonical deep copy isolates the immutable request from callback edits.
        bound=json.loads(json.dumps(binding,sort_keys=True,allow_nan=False))
        bhash=digest({"binding":bound,"mode":mode})
        with self._tx() as c:
            now=local_time(self.clock());day=now.date().isoformat()
            if bound["trade_date"]!=day:raise ValueError("PAPER_ADMISSION_DATE_MISMATCH")
            old=c.execute("SELECT binding_hash,result_json FROM paper_admissions WHERE signal_id=?",(signal_id,)).fetchone()
            if old:
                if old[0]!=bhash:raise ValueError("PAPER_ADMISSION_BINDING_CONFLICT")
                return json.loads(old[1])
            snapshot={"observed_at":now.isoformat(),"account":self._account(c,day),
                "positions":self._positions(c,day),
                "open_orders":tuple({**json.loads(r["body_json"]),"status":r["status"]} for r in self._open(c)),
                "admissions":self._admissions(c,day),"risk_day":self._risk_day(c,day),
                "peak_nav":str(self._meta(c,'risk_peak_nav')),
                "global_halt":c.execute("SELECT value FROM paper_meta WHERE key='risk_halt_reason'").fetchone()[0],
                "fills_today":tuple(dict(r) for r in c.execute("SELECT * FROM paper_fills WHERE substr(minute_end,1,10)=? ORDER BY fill_id",(day,)))}
            response=decide(snapshot)
            if (not isinstance(response,dict) or not {'judgment','order'}.issubset(response)
                    or set(response)-{'judgment','order','halt_reason','cancel_order_ids'} or not isinstance(response["judgment"],dict)):
                raise ValueError("PAPER_ADMISSION_DECISION_INVALID")
            # Validate persisted judgment data before any paper order is placed.
            judgment=json.loads(json.dumps(response["judgment"],sort_keys=True,allow_nan=False))
            halt=response.get('halt_reason')
            if halt is not None:
                if halt not in {'DAILY_LOSS_HALT','DRAWDOWN_HALT','A6_DEGRADED_HALT'} or response['order'] is not None:
                    raise ValueError('PAPER_HALT_REQUEST_INVALID')
                basis=self._risk_day(c,day)
                if basis is None:raise ValueError('PAPER_RISK_BASIS_MISSING')
                reasons=sorted(set(basis['halt_reason_codes'])|{halt})
                c.execute('UPDATE paper_risk_days SET halt_reasons_json=? WHERE trade_date=?',(dump(reasons),day))
                if halt=='DRAWDOWN_HALT':self._set(c,'risk_halt_reason',halt)
            order=response["order"];oid=None
            requested=response.get('cancel_order_ids',[]);cancelled=[]
            if (not isinstance(requested,list) or any(not isinstance(x,str) for x in requested)
                    or len(set(requested))!=len(requested) or (requested and order is not None)):
                raise ValueError('PAPER_CANCEL_SCOPE_INVALID')
            for cancel_id in requested:
                target=c.execute('SELECT * FROM paper_orders WHERE order_id=?',(cancel_id,)).fetchone()
                if (target is None or target['plan_id']!=bound['plan_id'] or target['symbol']!=bound['symbol']
                        or target['trade_date']!=day or target['status'] not in {'OPEN','PARTIAL'}
                        or self._order(target).side!='BUY'):
                    raise ValueError('PAPER_CANCEL_SCOPE_INVALID')
                if mode=='control':
                    self._cancel_order_tx(c,cancel_id);cancelled.append(cancel_id)
            if order is not None:
                if (not isinstance(order,dict) or set(order)-{"side","qty","order_type","limit_price","no_chase_price","tradable"}
                        or judgment.get("status") not in {"APPROVED","REDUCED"}):
                    raise ValueError("PAPER_ADMISSION_ORDER_INVALID")
                if mode=="control":
                    oid=self._place_order_tx(c,bound["symbol"],plan_id=bound["plan_id"],signal_id=signal_id,
                        client_order_id="team-"+digest(signal_id)[:32],now=now,**order)
            result={"signal_id":signal_id,"binding":bound,"judgment":judgment,"mode":mode,
                "order_id":oid,"order_side":order["side"] if oid else None,"dispatched":oid is not None,
                "observed_at":now.isoformat(),"cancelled_order_ids":cancelled}
            c.execute("INSERT INTO paper_admissions VALUES(?,?,?,?)",(signal_id,bhash,day,dump(result)))
            self._event(c,"admission:"+signal_id,"SIGNAL_ADMISSION",result)
            return result

    @staticmethod
    def _event(c,key,kind,payload):
        c.execute("INSERT INTO exec_ledger VALUES (?,?,?)",(key,kind,dump(payload)))

    def cancel_order(self,order_id):
        with self._tx() as c:
            return self._cancel_order_tx(c,order_id)

    def _cancel_order_tx(self,c,order_id):
        row = c.execute("SELECT * FROM paper_orders WHERE order_id=?",(order_id,)).fetchone()
        if row is None: raise ValueError("PAPER_ORDER_NOT_FOUND")
        if row["status"] in ("OPEN","PARTIAL"):
            c.execute("UPDATE paper_orders SET status='CANCELLED' WHERE order_id=?",(order_id,))
            self._event(c,"cancel:"+order_id,"ORDER_CANCELLED",{"order_id":order_id})
        return {"order_id":order_id,"status":"CANCELLED" if row["status"] in ("OPEN","PARTIAL") else row["status"]}

    def on_bar(self,bar:MinuteBar,rules:PaperRules,*,observed_at:datetime):
        day = local_time(bar.minute_end).date().isoformat(); end = local_time(bar.minute_end).isoformat()
        if local_time(bar.minute_end)>local_time(observed_at):raise ValueError("PAPER_FUTURE_BAR")
        if rules.trade_date != day: raise ValueError("PAPER_RULE_SNAPSHOT_DATE_MISMATCH")
        if not bar.closed or not bar.tradability_known:raise ValueError("PAPER_BAR_NOT_READY")
        if dump({k:v for k,v in asdict(rules).items() if k not in ('trade_date','trading_day')})!=dump(self.policy):
            raise ValueError("PAPER_RULE_POLICY_CONFLICT")
        bhash = digest({"bar":asdict(bar),"rules":asdict(rules)}); fills=[]
        with self._tx() as c:
            old = c.execute("SELECT sha256 FROM paper_bars WHERE symbol=? AND minute_end=?",(bar.symbol,end)).fetchone()
            if old:
                if old[0] != bhash: raise ValueError("PAPER_BAR_CONFLICT")
                return ()
            if c.execute("SELECT 1 FROM daily_nav WHERE trade_date=?",(day,)).fetchone():raise ValueError("PAPER_DAY_SETTLED")
            latest = c.execute("SELECT max(minute_end) FROM paper_bars WHERE symbol=?",(bar.symbol,)).fetchone()[0]
            if latest and end<latest: raise ValueError("PAPER_BAR_OUT_OF_ORDER")
            c.execute("INSERT INTO paper_bars VALUES (?,?,?,?)",(bar.symbol,end,bhash,local_time(observed_at).isoformat()))
            if bar.closed and bar.close is not None and bar.close>0 and bar.volume_shares>0:
                c.execute("INSERT INTO paper_marks VALUES (?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET price=excluded.price,trade_date=excluded.trade_date,minute_end=excluded.minute_end",(bar.symbol,str(bar.close),day,end))
            volume_left=int(Decimal(bar.volume_shares)*rules.participation)
            for row in self._open(c):
                if row["symbol"] != bar.symbol: continue
                if row["trade_date"]<day:
                    c.execute("UPDATE paper_orders SET status='CANCELLED' WHERE order_id=?",(row["order_id"],));continue
                o=self._order(row)
                if o.side=="SELL": o=replace(o,sellable_qty=min(o.remaining_qty,self._sellable(c,o.symbol,o.plan_id,day)))
                result=match_order(o,bar,rules,spendable_cash=max(ZERO,self._meta(c,"cash")-self._reserved(c,o.order_id)),available_volume_shares=volume_left)
                if not result.qty: continue
                gross=result.price*result.qty; cash=self._meta(c,"cash")
                fid="fill-"+digest([o.order_id,end,o.qty-o.remaining_qty,result.qty])[:32]
                if o.side=="BUY":
                    cash-=gross+result.fee
                    c.execute("INSERT INTO paper_lots VALUES (?,?,?,?,?,?)",(fid,o.symbol,o.plan_id,day,result.qty,str(gross+result.fee)))
                else:
                    remaining=result.qty; cost=ZERO
                    lots=c.execute("SELECT * FROM paper_lots WHERE symbol=? AND plan_id=? AND open_date<? AND qty>0 ORDER BY open_date,lot_id",(o.symbol,o.plan_id,day)).fetchall()
                    for lot in lots:
                        take=min(remaining,lot["qty"])
                        allocated=Decimal(lot["cost"]) if take==lot["qty"] else money(Decimal(lot["cost"])*take/lot["qty"])
                        c.execute("UPDATE paper_lots SET qty=?,cost=? WHERE lot_id=?",(lot["qty"]-take,str(Decimal(lot["cost"])-allocated),lot["lot_id"]))
                        remaining-=take;cost+=allocated
                        if not remaining:break
                    if remaining:raise ValueError("PAPER_LOT_STATE_CONFLICT")
                    cash+=gross-result.fee;self._set(c,"realized",self._meta(c,"realized")+gross-result.fee-cost)
                if cash<0:raise ValueError("PAPER_NEGATIVE_CASH")
                self._set(c,"cash",cash)
                updated=replace(o,remaining_qty=o.remaining_qty-result.qty,filled_notional=o.filled_notional+gross,fees_paid=o.fees_paid+result.fee)
                c.execute("UPDATE paper_orders SET status=?,body_json=? WHERE order_id=?",(result.status,dump(asdict(updated)),o.order_id))
                fill=dict(fill_id=fid,order_id=o.order_id,symbol=o.symbol,plan_id=o.plan_id,side=o.side,qty=result.qty,price=str(result.price),fee=str(result.fee),minute_end=end,run_id=self.run_id,source_signal_id=o.signal_id)
                c.execute("INSERT INTO paper_fills VALUES (?,?,?,?,?,?,?,?,?,?,?)",tuple(fill.values()))
                self._event(c,fid,"FILL",fill);fills.append(fill);volume_left-=result.qty
            positions=self._positions(c,day)
            if all(p['mark_minute_end']==end for p in positions):
                nav=self._account(c,day)['nav']
                if nav is not None:self._set(c,'risk_peak_nav',max(self._meta(c,'risk_peak_nav'),Decimal(nav)))
        return tuple(fills)

    def _positions(self,c,day):
        groups=defaultdict(lambda:{"qty":0,"sellable_qty":0,"cost":ZERO})
        for row in c.execute("SELECT * FROM paper_lots WHERE qty>0 ORDER BY symbol,plan_id,lot_id"):
            p=groups[(row["symbol"],row["plan_id"])];p["qty"]+=row["qty"];p["cost"]+=Decimal(row["cost"])
            if row["open_date"]<day:p["sellable_qty"]+=row["qty"]
        marks={r["symbol"]:r for r in c.execute("SELECT * FROM paper_marks")};result=[]
        for (symbol,plan_id),p in sorted(groups.items()):
            mark=marks.get(symbol);value=Decimal(mark["price"])*p["qty"] if mark else None
            result.append(dict(symbol=symbol,plan_id=plan_id,qty=p["qty"],sellable_qty=p["sellable_qty"],avg_cost=str(p["cost"]/p["qty"]),market_value=str(value) if value is not None else None,mark_trade_date=mark["trade_date"] if mark else None,mark_minute_end=mark["minute_end"] if mark else None))
        return tuple(result)

    def get_positions(self):
        with self._tx(readonly=True) as c:return self._positions(c,local_time(self.clock()).date().isoformat())

    def get_account(self):
        with self._tx(readonly=True) as c:
            return self._account(c,local_time(self.clock()).date().isoformat())

    def planning_snapshot(self):
        """One consistent owned-account view; no mark or baseline is invented."""
        with self._tx(readonly=True) as c:
            day=local_time(self.clock()).date().isoformat()
            return dict(account=self._account(c,day),positions=self._positions(c,day),
                risk_day=self._risk_day(c,day),peak_nav=str(self._meta(c,'risk_peak_nav')),
                global_halt=c.execute("SELECT value FROM paper_meta WHERE key='risk_halt_reason'").fetchone()[0])

    def _account(self,c,day):
        positions=self._positions(c,day);cash=self._meta(c,"cash")
        value=sum((Decimal(p["market_value"]) for p in positions),ZERO) if all(p["market_value"] is not None for p in positions) else None
        cost=sum((Decimal(r[0]) for r in c.execute("SELECT cost FROM paper_lots WHERE qty>0")),ZERO)
        return dict(cash=str(cash),nav=str(cash+value) if value is not None else None,positions_value=str(value) if value is not None else None,frozen=str(self._reserved(c)),pnl_realized=str(self._meta(c,"realized")),pnl_unrealized=str(value-cost) if value is not None else None)

    def get_orders(self,trade_date):
        with self._tx(readonly=True) as c:
            return tuple({**json.loads(r["body_json"]),"status":r["status"],"client_order_id":r["client_order_id"]} for r in c.execute("SELECT * FROM paper_orders WHERE trade_date=? ORDER BY order_id",(trade_date,)))

    def get_fills(self,trade_date):
        with self._tx(readonly=True) as c:
            return tuple(dict(r) for r in c.execute("SELECT * FROM paper_fills WHERE substr(minute_end,1,10)=? ORDER BY fill_id",(trade_date,)))

    def reconcile(self):
        with self._tx(readonly=True) as c:
            return self._reconcile(c)

    def _reconcile(self,c):
        expected=self._meta(c,"initial_capital");quantity=defaultdict(int);issues=[]
        for f in c.execute("SELECT * FROM paper_fills ORDER BY minute_end,fill_id"):
            amount=Decimal(f["price"])*f["qty"];expected+=(amount if f["side"]=="SELL" else -amount)-Decimal(f["fee"])
            quantity[(f["symbol"],f["plan_id"])]+=f["qty"]*(1 if f["side"]=="BUY" else -1)
        actual=defaultdict(int)
        for r in c.execute("SELECT * FROM paper_lots"):
            actual[(r["symbol"],r["plan_id"])]+=r["qty"]
            if r["qty"]<0 or Decimal(r["cost"])<0:issues.append("NEGATIVE_LOT")
        if expected!=self._meta(c,"cash"):issues.append("CASH_LEDGER_MISMATCH")
        if any(actual[k]!=quantity[k] for k in set(actual)|set(quantity)):issues.append("POSITION_LEDGER_MISMATCH")
        if self._reserved(c)>self._meta(c,"cash"):issues.append("RESERVED_CASH_EXCEEDS_CASH")
        return {"status":"MISMATCH" if issues else "MATCH","reason_codes":sorted(set(issues))}

    def settle(self,trade_date):
        now=local_time(self.clock())
        if now.date().isoformat()!=trade_date or now.time().replace(tzinfo=None)<time(15,30):
            raise ValueError("PAPER_SETTLEMENT_TIME_INVALID")
        with self._tx() as c:
            old=c.execute("SELECT payload_json FROM daily_nav WHERE trade_date=?",(trade_date,)).fetchone()
            if old:return json.loads(old[0])
            for row in self._open(c):
                if row['trade_date']<=trade_date:
                    c.execute("UPDATE paper_orders SET status='CANCELLED' WHERE order_id=?",(row['order_id'],))
                    self._event(c,'eod-cancel:'+row['order_id'],'ORDER_EXPIRED',{'order_id':row['order_id'],'trade_date':trade_date})
            positions=self._positions(c,trade_date)
            final_marks={r['symbol'] for r in c.execute("SELECT symbol,minute_end FROM paper_marks WHERE trade_date=?",(trade_date,))
                         if local_time(datetime.fromisoformat(r['minute_end'])).time().replace(tzinfo=None)==time(15)}
            missing=[p['symbol'] for p in positions if p['symbol'] not in final_marks]
            account=self._account(c,trade_date)
            if missing:
                account={**account,'nav':None,'positions_value':None,'pnl_unrealized':None}
            report=dict(trade_date=trade_date,status='DATA_LIMITED' if missing else 'COMPLETE',account=account,
                        positions=list(positions),missing_current_marks=sorted(set(missing)),reconcile=self._reconcile(c))
            c.execute("INSERT INTO daily_nav VALUES (?,?)",(trade_date,dump(report)))
            self._event(c,'settle:'+trade_date,'SETTLEMENT',report)
            return report

    def snapshot_bytes(self):
        with self._tx(readonly=True) as c:
            tables=("paper_meta","paper_orders","paper_fills","paper_lots","paper_bars","paper_marks","exec_ledger","daily_nav","paper_admissions","paper_risk_days")
            data={table:[dict(r) for r in c.execute("SELECT * FROM "+table+" ORDER BY 1,2")] for table in tables}
            return dump(data).encode()
