def is_upper_buy_closed(history):
    return (history['last'] > history['open'] * 1.068).all()


def is_upper_buy_all_day(history):
    return (history['low'] > history['open'] * 1.068).all()
