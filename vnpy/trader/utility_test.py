

from vnpy.trader.utility import floor_to, round_to


def test_round_to():
    res = round_to(1.234, 1)
    print("resresresresre")
    print(res)


def test_floor_to():
    res = floor_to(1.234, 1)
    print("resresresresre")
    print(res)


if __name__ == "__main__":
    test_round_to()
    test_floor_to()