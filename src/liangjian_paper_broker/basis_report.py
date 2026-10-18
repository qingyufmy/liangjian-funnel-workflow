"""Same-account fill cohorts, not two fictitious independently funded NAVs.

A round trip belongs to strict VWAP only when both sides are reported. Mixed
and unknown legacy fills never acquire strict performance through aggregation.
Reads supplied ledger rows; no database, clock, model or source side effects.
"""
from decimal import Decimal as D
from .matching import money

STRICT='REPORTED_VWAP';ESTIMATED='OHLC_ESTIMATED_PAPER_FILL';UNKNOWN='LEGACY_UNCLASSIFIED'


def fill_basis_report(fills,lots,marks,*,trade_date):
    def basis(fill):return fill.get('fill_basis') if fill.get('fill_basis') in {STRICT,ESTIMATED} else UNKNOWN
    daily={key:dict(fill_count=0,buy_qty=0,sell_qty=0,gross_buy=D(0),gross_sell=D(0),fees=D(0),cash_flow=D(0))
        for key in (STRICT,ESTIMATED,UNKNOWN)}
    performance={key:dict(closed_qty=0,realized_pnl=D(0)) for key in (STRICT,ESTIMATED,'MIXED_BASIS',UNKNOWN)}
    inventory={};original={}
    for fill in sorted(fills,key=lambda f:(f['minute_end'],f['fill_id'])):
        if fill['minute_end'][:10]>trade_date:continue
        kind=basis(fill);quantity=fill['qty'];gross=D(fill['price'])*quantity;fee=D(fill['fee'])
        if fill['minute_end'][:10]==trade_date:
            group=daily[kind];side=fill['side'].lower();group['fill_count']+=1
            group[side+'_qty']+=quantity;group['gross_'+side]+=gross;group['fees']+=fee
            group['cash_flow']+=(gross if side=='sell' else -gross)-fee
        key=(fill['symbol'],fill['plan_id']);owned=inventory.setdefault(key,[])
        if fill['side']=='BUY':
            row=dict(fill_id=fill['fill_id'],day=fill['minute_end'][:10],qty=quantity,cost=gross+fee,basis=kind)
            owned.append(row);original[fill['fill_id']]=kind
            owned.sort(key=lambda r:(r['day'],r['fill_id']))
            continue
        remaining=quantity;fee_left=fee
        for lot in owned:
            if remaining<=0:break
            if not lot['qty'] or lot['day']>=fill['minute_end'][:10]:continue
            take=min(remaining,lot['qty'])
            cost=lot['cost'] if take==lot['qty'] else money(lot['cost']*take/lot['qty'])
            allocated_fee=fee_left if take==remaining else money(fee_left*take/remaining)
            cohort=UNKNOWN if UNKNOWN in (kind,lot['basis']) else kind if kind==lot['basis'] else 'MIXED_BASIS'
            performance[cohort]['closed_qty']+=take
            performance[cohort]['realized_pnl']+=D(fill['price'])*take-allocated_fee-cost
            lot['qty']-=take;lot['cost']-=cost;remaining-=take;fee_left-=allocated_fee
        if remaining:raise ValueError('PAPER_BASIS_LOT_RECONCILIATION_FAILED')
    unrealized={key:dict(qty=0,cost=D(0),market_value=D(0),unrealized_pnl=D(0),missing_marks=[])
        for key in (STRICT,ESTIMATED,UNKNOWN)}
    for lot in lots:
        if lot['qty']<=0:continue
        kind=original.get(lot['lot_id'],UNKNOWN);group=unrealized[kind]
        group['qty']+=lot['qty'];group['cost']+=D(lot['cost'])
        mark=marks.get(lot['symbol'])
        if mark is None or mark['trade_date']!=trade_date or mark['minute_end'][11:16]!='15:00':
            group['missing_marks'].append(lot['symbol']);continue
        value=D(mark['price'])*lot['qty'];group['market_value']+=value
        group['unrealized_pnl']+=value-D(lot['cost'])
    for group in unrealized.values():
        group['missing_marks']=sorted(set(group['missing_marks']))
        if group['missing_marks']:group.update(market_value=None,unrealized_pnl=None)
    return dict(schema_version='paper-fill-basis/1',daily_by_fill_basis=daily,performance_cohorts=performance,
        open_by_entry_basis=unrealized,strict_vwap_requires_both_sides_reported=True,
        same_shared_account=True,separate_funded_account_curve_claimed=False,
        performance_period='ACCOUNT_INCEPTION_THROUGH_TRADE_DATE',
        strict_win_rate=None,strict_return=None,performance_status='ATTRIBUTED_PNL_NOT_RETURN_OR_WIN_RATE')
