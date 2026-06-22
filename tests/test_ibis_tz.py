from dsgrid.ibis.tz import custom_time_zone, get_current_time_zone, set_current_time_zone


def test_current_time_zone_contexts():
    original = get_current_time_zone()
    try:
        set_current_time_zone("UTC")
        assert get_current_time_zone() == "UTC"
        with custom_time_zone("America/Denver"):
            assert get_current_time_zone() == "America/Denver"
        assert get_current_time_zone() == "UTC"
    finally:
        set_current_time_zone(original)
