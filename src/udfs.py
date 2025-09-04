# Pure-Python helpers suitable for unit testing (kept minimal)
def clip_amount(amount, min_v=0.0, max_v=10000.0):
    if amount is None:
        return None
    return max(min(float(amount), max_v), min_v)

def is_amount_in_range(amount, min_v=0.0, max_v=10000.0):
    if amount is None:
        return False
    a = float(amount)
    return (a >= min_v) and (a <= max_v)
