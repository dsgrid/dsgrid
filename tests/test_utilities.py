"""Tests for dsgrid.utils.utilities module."""

from dsgrid.utils.utilities import make_unique_key, sorted_with_nulls


def test_make_unique_key_new_name():
    """Test that a new name is returned as-is."""
    existing = {"file", "other"}
    result = make_unique_key("new_name", existing)
    assert result == "new_name"


def test_make_unique_key_empty_existing():
    """Test with empty existing keys."""
    result = make_unique_key("file", set())
    assert result == "file"


def test_make_unique_key_first_duplicate():
    """Test that first duplicate gets _1 suffix."""
    existing = {"file", "other"}
    result = make_unique_key("file", existing)
    assert result == "file_1"


def test_make_unique_key_multiple_duplicates():
    """Test that subsequent duplicates get incrementing suffixes."""
    existing = {"file", "file_1", "file_2"}
    result = make_unique_key("file", existing)
    assert result == "file_3"


def test_make_unique_key_gap_in_sequence():
    """Test that gaps in the sequence are filled."""
    existing = {"file", "file_2", "file_3"}
    result = make_unique_key("file", existing)
    assert result == "file_1"


def test_make_unique_key_with_dict():
    """Test that dict keys work as existing_keys."""
    existing = {"file": 1, "file_1": 2}
    result = make_unique_key("file", existing)
    assert result == "file_2"


def test_make_unique_key_with_list():
    """Test that list works as existing_keys."""
    existing = ["file", "file_1"]
    result = make_unique_key("file", existing)
    assert result == "file_2"


def test_make_unique_key_preserves_underscores():
    """Test that names with underscores are handled correctly."""
    existing = {"my_file", "my_file_1"}
    result = make_unique_key("my_file", existing)
    assert result == "my_file_2"


def test_make_unique_key_similar_names():
    """Test that similar but different names don't conflict."""
    existing = {"file", "file_name", "file_other"}
    result = make_unique_key("file", existing)
    assert result == "file_1"

    result2 = make_unique_key("file_name", existing)
    assert result2 == "file_name_1"


def test_sorted_with_nulls_no_nulls():
    """Test that values without None sort normally."""
    assert sorted_with_nulls({"b", "a", "c"}) == ["a", "b", "c"]


def test_sorted_with_nulls_none_first():
    """Test that None sorts before all other values."""
    assert sorted_with_nulls({"b", None, "a"}) == [None, "a", "b"]


def test_sorted_with_nulls_only_none():
    """Test a collection containing only None."""
    assert sorted_with_nulls([None, None]) == [None]


def test_sorted_with_nulls_empty():
    """Test an empty collection."""
    assert sorted_with_nulls(set()) == []


def test_sorted_with_nulls_ints():
    """Test that non-string values sort by value."""
    assert sorted_with_nulls({3, None, 1, 2}) == [None, 1, 2, 3]
