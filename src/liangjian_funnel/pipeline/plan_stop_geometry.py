"""1116 D1: minimum stop geometry, using the existing upper-price denominator."""
from decimal import Decimal, localcontext
import math

POLICY = 'minimum-stop-geometry/1'
MIN_LOW_DISTANCE = Decimal('.01')
MIN_TOTAL_DISTANCE = Decimal('.02')

def minimum_stop(low, high, original_stop):
    values=[Decimal(str(v)) for v in (low,high,original_stop)]
    if any(not v.is_finite() or v<=0 for v in values) or not values[2]<values[0]<=values[1]:
        raise ValueError('MINIMUM_STOP_GEOMETRY_INVALID')
    low,high,stop=values
    with localcontext() as ctx:
        ctx.prec=50
        cap=min(stop,low*(1-MIN_LOW_DISTANCE),high*(1-MIN_TOTAL_DISTANCE))
    value=float(cap)
    if Decimal(str(value))>cap:value=math.nextafter(value,-math.inf)
    return value

def derived_geometry(low, high, stop, target):
    low,high,stop,target=(Decimal(str(v)) for v in (low,high,stop,target))
    if any(not v.is_finite() or v<=0 for v in (low,high,stop,target)) or not stop<low<=high<target:
        raise ValueError('DERIVED_PLAN_GEOMETRY_INVALID')
    with localcontext() as ctx:
        ctx.prec=50
        return dict(reward_risk=float((target-high)/(high-stop)),
                    stop_distance_pct=float((high-stop)/high),
                    invalidation_distance_from_low_pct=float((low-stop)/low))
