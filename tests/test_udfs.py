import pytest
from src.udfs import clip_amount, is_amount_in_range

def test_clip_amount_basic():
    assert clip_amount(50, 0, 100) == 50
    assert clip_amount(-5, 0, 100) == 0
    assert clip_amount(150, 0, 100) == 100

def test_is_amount_in_range():
    assert is_amount_in_range(5, 0, 10) is True
    assert is_amount_in_range(0, 0, 10) is True
    assert is_amount_in_range(10, 0, 10) is True
    assert is_amount_in_range(-1, 0, 10) is False
    assert is_amount_in_range(11, 0, 10) is False
