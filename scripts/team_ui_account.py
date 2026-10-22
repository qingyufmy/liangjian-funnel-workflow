"""Display-only dated lots/orders. No broker objects, marks, calendar or writes."""
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import json


def quantity(value):
    if type(value) is not int or value < 0:
        raise ValueError('PAPER_QUANTITY_UNPROVEN')
    return value


def project_account(root, query, out, *, connect, source, scalar, number, connection=None):
    day=query['trade_date'];limit=query['limit'];offset=query['offset']
    owns_connection=connection is None
    db=connect(root,'state/team_v2/paper.db') if owns_connection else connection
    try:
        if owns_connection:db.execute('BEGIN')  # One read snapshot for lots and outstanding reservations.
        if query['kind']=='account/fills':
            next_day=(date.fromisoformat(day)+timedelta(days=1)).isoformat();rows=[]
            for original in db.execute('SELECT fill_id,order_id,symbol,plan_id,side,qty,price,fee,minute_end FROM paper_fills WHERE minute_end>=? AND minute_end<? ORDER BY minute_end,fill_id LIMIT ? OFFSET ?',(day,next_day,limit+1,offset)):
                row=dict(original);item={k:scalar(v,identifier=k not in {'qty','price','fee'}) for k,v in row.items()}
                item['source']=source('paper_fills',row['fill_id'],json.dumps(row,sort_keys=True));rows.append(item)
        elif query['kind']=='account/orders':
            next_day=(date.fromisoformat(day)+timedelta(days=1)).isoformat()
            rows=[]
            for original in db.execute('SELECT order_id,symbol,plan_id,trade_date,status,body_json FROM paper_orders WHERE trade_date>=? AND trade_date<? ORDER BY order_id LIMIT ? OFFSET ?', (day,next_day,limit+1,offset)):
                row=dict(original);body=json.loads(row.pop('body_json'))
                rows.append(dict(id=scalar(row['order_id'],identifier=True),code=scalar(row['symbol'],identifier=True),
                    plan_id=scalar(row['plan_id'],identifier=True),status=scalar(row['status'],identifier=True),
                    side=body.get('side') if body.get('side') in {'BUY','SELL'} else None,
                    qty=quantity(body['qty']) if 'qty' in body else None,
                    remaining_qty=quantity(body['remaining_qty']) if 'remaining_qty' in body else None,
                    limit_price=number(body.get('limit_price')),submitted_at=scalar(body.get('submitted_at'),identifier=True),
                    source=source('paper_orders',row['order_id'],original['body_json'])))
        else:
            lots=[dict(x) for x in db.execute('SELECT lot_id,symbol,plan_id,open_date,qty,cost FROM paper_lots ORDER BY symbol,plan_id,open_date,lot_id LIMIT 501')]
            if len(lots)>500:
                return dict(out,missing_reason='LOT_ROW_BUDGET_EXCEEDED')
            rows=[];groups=defaultdict(list)
            for lot in lots:
                qty=quantity(lot['qty']);opened=lot['open_date']
                if not isinstance(opened,str) or date.fromisoformat(opened).isoformat()!=opened or opened>day:
                    raise ValueError('DATED_LOT_UNPROVEN')
                if not qty:continue
                groups[(lot['symbol'],lot['plan_id'])].append(lot)
                if query['kind']=='account/lots':
                    rows.append(dict(id=scalar(lot['lot_id'],identifier=True),code=scalar(lot['symbol'],identifier=True),
                        plan_id=scalar(lot['plan_id'],identifier=True),open_date=opened,qty=qty,
                        sellable_qty=qty if opened<day else 0,t1_locked=opened==day,
                        sellable_from=None,sellable_from_missing_reason='NEXT_TRADING_DAY_NOT_PROJECTED',
                        source=source('paper_lots',lot['lot_id'],json.dumps(lot,sort_keys=True))))
            if query['kind']=='account/positions':
                orders=[dict(x) for x in db.execute("SELECT order_id,symbol,plan_id,body_json FROM paper_orders WHERE status IN ('OPEN','PARTIAL') ORDER BY order_id LIMIT 501")]
                if len(orders)>500:return dict(out,missing_reason='OPEN_ORDER_ROW_BUDGET_EXCEEDED')
                reserved=defaultdict(int);reservation_sources=defaultdict(list)
                for order in orders:
                    body=json.loads(order['body_json'])
                    if body.get('side')=='SELL':
                        key=(order['symbol'],order['plan_id']);reserved[key]+=quantity(body['remaining_qty'])
                        reservation_sources[key].append(source('paper_orders',order['order_id'],order['body_json']))
                for key,held_lots in groups.items():
                    qty=sum(x['qty'] for x in held_lots);dated=sum(x['qty'] for x in held_lots if x['open_date']<day)
                    try:
                        costs=[Decimal(x['cost']) for x in held_lots]
                        cost=sum(costs) if all(x.is_finite() and x>=0 for x in costs) else None
                    except (InvalidOperation,TypeError):cost=None
                    proof=json.dumps(dict(lots=held_lots,reservations=reservation_sources[key]),sort_keys=True)
                    rows.append(dict(code=scalar(key[0],identifier=True),plan_id=scalar(key[1],identifier=True),qty=qty,
                        dated_sellable_qty=dated,reserved_sell_qty=reserved[key],sellable_qty=max(0,dated-reserved[key]),
                        t1_locked_qty=qty-dated,t1_locked=qty!=dated,
                        cost=number(str(cost)) if cost is not None else None,
                        avg_cost=number(str(cost/qty)) if cost is not None else None,
                        price=None,unrealized_pnl=None,valuation_missing_reason='NO_APPROVED_SAME_TIME_MARK',
                        source=source('paper_lots',key[1],proof)))
            rows=rows[offset:offset+limit+1]
        from team_ui_presentation import decorate_names,receipt
        try: identities=(receipt(root,day) or {}).get('security_names',{})
        except (OSError,ValueError,KeyError,TypeError):identities={}
        decorate_names(root, rows[:limit], connect,identities)
        return dict(out,status='AVAILABLE',rows=rows[:limit],page_count=len(rows[:limit]),
            next_offset=offset+limit if len(rows)>limit else None,missing_reason=None,
            ledger_scope='CURRENT_REMAINING_LEDGER_NOT_HISTORICAL_RECONSTRUCTION',
            sellability_basis='DATED_LOTS_MINUS_OPEN_SELL_RESERVATIONS_NOT_EXECUTION_PERMISSION')
    finally:
        if owns_connection:db.rollback();db.close()
